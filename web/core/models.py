from django.db import models


class Route(models.TextChoices):
    PENDING = "pending", "Pending"
    PROCESSED = "processed", "Processed"     # OCR read and saved (no decision rules applied)
    ACCEPTED = "accepted", "Accepted"
    REVIEW = "review", "Review"
    NO_PLATE = "no_plate", "No plate"
    FAILED = "failed", "Failed"


class Job(models.Model):
    class Status(models.TextChoices):
        RUNNING = "running"
        DONE = "done"
        ERROR = "error"

    input_path = models.CharField(max_length=500)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RUNNING)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    folder_count = models.PositiveIntegerField(default=0)
    error = models.TextField(blank=True)

    def __str__(self):
        return f"Job #{self.pk} ({self.status})"


class Folder(models.Model):
    # job is empty for folders processed by bots (bots work from the queue, not in "jobs")
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="folders", null=True, blank=True)
    path = models.CharField(max_length=500, db_index=True)   # indexed: looked up by path
    processed_by = models.CharField(max_length=100, blank=True)  # bot name, or "" for Prefect runs
    name = models.CharField(max_length=255, db_index=True)
    route = models.CharField(max_length=20, choices=Route.choices, default=Route.PENDING, db_index=True)
    scenario = models.CharField(max_length=5, blank=True, db_index=True)
    reason = models.TextField(blank=True)
    final_plate = models.CharField(max_length=20, blank=True, db_index=True)
    final_score = models.FloatField(null=True, blank=True)
    best_image = models.ForeignKey("Image", null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    candidates = models.JSONField(default=dict, blank=True)
    # human review
    reviewed_plate = models.CharField(max_length=20, blank=True)
    reviewed_by = models.CharField(max_length=150, blank=True)
    reviewed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name


class Image(models.Model):
    folder = models.ForeignKey(Folder, on_delete=models.CASCADE, related_name="images")
    path = models.CharField(max_length=500)
    file_name = models.CharField(max_length=255)
    sha256 = models.CharField(max_length=64, db_index=True)
    is_duplicate = models.BooleanField(default=False)
    is_corrupt = models.BooleanField(default=False)
    quality = models.FloatField(null=True, blank=True)
    quality_details = models.JSONField(default=dict, blank=True)
    plate = models.CharField(max_length=20, blank=True)
    confidence = models.FloatField(null=True, blank=True)
    final_score = models.FloatField(null=True, blank=True)
    api_response = models.JSONField(default=dict, blank=True)
    api_error = models.TextField(blank=True)
    # Timing (empty for images processed before timing was added):
    duration_ms = models.PositiveIntegerField(null=True, blank=True)       # rate-limit wait + read + OCR + retries
    api_attempts = models.PositiveSmallIntegerField(null=True, blank=True)  # OCR calls made (1 = first try)
    processed_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.file_name


# ============================================================================ bots
class Bot(models.Model):
    """One worker process of the assembly line. Every bot belongs to ONE stage
    (reader, preprocess, ocr, llm or trip) and only takes items from that stage's line.

    The supervisor reads this table: desired_running=True means "this bot should be
    running", and the bot itself reports its real state with a heartbeat.
    """

    class State(models.TextChoices):
        STOPPED = "stopped", "Stopped"
        STARTING = "starting", "Starting"
        RUNNING = "running", "Running"
        STOPPING = "stopping", "Stopping"
        CRASHED = "crashed", "Crashed"

    class Stage(models.TextChoices):
        READER = "reader", "Reading"
        PREPROCESS = "preprocess", "Preprocessing"
        OCR = "ocr", "OCR"
        LLM = "llm", "LLM"
        TRIP = "trip", "Trip"

    name = models.SlugField(max_length=50, unique=True)   # letters, numbers, - and _ only
    stage = models.CharField(max_length=12, choices=Stage.choices, default=Stage.OCR, db_index=True)
    shard = models.CharField(max_length=10, blank=True)   # reader bots only, e.g. "2/3"; set by the supervisor
    # The bot's own account for the outside service its stage calls (the OCR API for OCR bots).
    api_username = models.CharField(max_length=150, blank=True)
    api_password_encrypted = models.TextField(blank=True)  # never stored as plain text
    rate_limit = models.FloatField(default=0)              # API calls per second for this bot, 0 = no limit

    # What we WANT (set from React)
    desired_running = models.BooleanField(default=False)
    delete_requested = models.BooleanField(default=False)

    # What IS happening (reported by the bot / supervisor)
    state = models.CharField(max_length=10, choices=State.choices, default=State.STOPPED)
    pid = models.PositiveIntegerField(null=True, blank=True)
    host = models.CharField(max_length=255, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    last_heartbeat = models.DateTimeField(null=True, blank=True)
    current_task = models.TextField(blank=True)            # what it is doing right now
    processed = models.PositiveIntegerField(default=0)
    failed = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True)
    restarts = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    def set_password(self, raw: str) -> None:
        from .crypto import encrypt
        self.api_password_encrypted = encrypt(raw)

    def get_password(self) -> str:
        from .crypto import decrypt
        return decrypt(self.api_password_encrypted)

    def __str__(self):
        return self.name


class BotEvent(models.Model):
    """A short log line from a bot or the supervisor, shown on the Bots page."""
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="events")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    level = models.CharField(max_length=10, default="info")   # info / warning / error
    message = models.TextField()

    class Meta:
        ordering = ["-created_at"]


# ============================================================================ intake + assembly line
class IntakeFolder(models.Model):
    """One vehicle folder found by a reader bot.

    Its `stage` says where it is on the assembly line:
      reading       its images are still being preprocessed / read by OCR
      needs_llm     OCR finished; only 2 of 4 engines agreed, so the LLM decides
      llm_running   an LLM bot is working on it
      needs_trip    the plate is decided; a trip must be created
      trip_running  a trip bot is working on it
      manual        a person must check it on the Review page
      done          trip created
      failed        it kept failing and was given up
    """

    class Stage(models.TextChoices):
        READING = "reading", "Images in progress"
        NEEDS_LLM = "needs_llm", "Waiting for LLM"
        LLM_RUNNING = "llm_running", "LLM working"
        NEEDS_TRIP = "needs_trip", "Waiting for trip"
        TRIP_RUNNING = "trip_running", "Trip being created"
        MANUAL = "manual", "Manual review"
        DONE = "done", "Done"
        FAILED = "failed", "Failed"

    class Accuracy(models.TextChoices):
        LOW = "low", "Low accuracy"
        HIGH = "high", "High accuracy"

    path = models.CharField(max_length=500, unique=True)    # unique: registered once, even with many readers
    name = models.CharField(max_length=255, db_index=True)
    source = models.CharField(max_length=100, db_index=True)  # which configured source it came from
    accuracy = models.CharField(max_length=4, choices=Accuracy.choices)
    priority = models.PositiveSmallIntegerField(db_index=True)  # lower = first (low accuracy before high)
    stage = models.CharField(max_length=12, choices=Stage.choices, default=Stage.READING, db_index=True)
    # queue bookkeeping, used while an LLM or trip bot holds the folder
    taken_by = models.CharField(max_length=50, blank=True)
    taken_at = models.DateTimeField(null=True, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    last_error = models.TextField(blank=True)
    failed_in = models.CharField(max_length=12, blank=True)       # stage it failed in (for "Retry")
    route_reason = models.CharField(max_length=255, blank=True)   # why it went to trip / llm / manual
    # OCR result of the folder: the highest-confidence plate any of its images gave
    plate = models.CharField(max_length=20, blank=True)
    confidence = models.FloatField(null=True, blank=True)
    read_at = models.DateTimeField(null=True, blank=True)    # when its last image was through OCR
    image_count = models.PositiveIntegerField(default=0)
    duplicate_count = models.PositiveIntegerField(default=0)
    total_bytes = models.BigIntegerField(default=0)
    folder_modified_at = models.DateTimeField(null=True)
    discovered_at = models.DateTimeField(auto_now_add=True, db_index=True)
    discovered_by = models.CharField(max_length=100, blank=True)   # which reader registered it
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        # "the next folders in this line": filter by stage, then priority, then oldest
        indexes = [models.Index(fields=["stage", "priority", "id"], name="folder_line_idx")]

    def __str__(self):
        return f"{self.name} ({self.accuracy}, {self.stage})"


class IntakeImage(models.Model):
    """One image inside an IntakeFolder, travelling through the image lines:

      new  ->  preprocessing  ->  cleaned  ->  ocr_running  ->  read
      (skipped: a duplicate of another image in the folder; failed: given up)
    """

    class Stage(models.TextChoices):
        NEW = "new", "Waiting for preprocessing"
        PREPROCESSING = "preprocessing", "Preprocessing"
        CLEANED = "cleaned", "Waiting for OCR"
        OCR_RUNNING = "ocr_running", "OCR working"
        READ = "read", "Read"
        SKIPPED = "skipped", "Skipped (duplicate)"
        FAILED = "failed", "Failed"

    folder = models.ForeignKey(IntakeFolder, on_delete=models.CASCADE, related_name="images")
    file_name = models.CharField(max_length=255)
    path = models.CharField(max_length=500)
    size_bytes = models.BigIntegerField()
    sha256 = models.CharField(max_length=64, db_index=True)   # fingerprint: same content = same hash
    is_duplicate = models.BooleanField(default=False)        # same content as another image in this folder
    modified_at = models.DateTimeField()
    # assembly line
    stage = models.CharField(max_length=14, choices=Stage.choices, default=Stage.NEW, db_index=True)
    priority = models.PositiveSmallIntegerField(default=0)   # copied from the folder: low accuracy first
    taken_by = models.CharField(max_length=50, blank=True)   # bot holding it right now
    taken_at = models.DateTimeField(null=True, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    last_error = models.TextField(blank=True)
    failed_in = models.CharField(max_length=12, blank=True)       # stage it failed in (for "Retry")
    cleaned_path = models.CharField(max_length=500, blank=True)   # written by the preprocessing stage
    # written by the OCR stage
    plate = models.CharField(max_length=20, blank=True)
    confidence = models.FloatField(null=True, blank=True)
    api_response = models.JSONField(default=dict, blank=True)
    read_by = models.CharField(max_length=50, blank=True)              # the OCR bot that read it
    duration_ms = models.PositiveIntegerField(null=True, blank=True)   # rate-limit wait + OCR call + retries
    api_attempts = models.PositiveSmallIntegerField(null=True, blank=True)  # OCR calls made (1 = first try)

    class Meta:
        ordering = ["file_name"]
        indexes = [models.Index(fields=["stage", "priority", "id"], name="image_line_idx")]


class StageMinute(models.Model):
    """How many items each stage finished (or failed) per minute: the speed shown in the UI."""
    stage = models.CharField(max_length=12)
    minute = models.DateTimeField()
    done = models.PositiveIntegerField(default=0)
    failed = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["stage", "minute"], name="stage_minute_unique")]


class StageSettings(models.Model):
    """Settings of one assembly-line stage changed on the Pipeline page.

    config/settings.yaml ("stages:") holds the defaults; a value saved here wins.
    Bots read the merged settings before every batch, so a change applies within seconds,
    without restarting anything.
    """
    stage = models.CharField(max_length=12, unique=True)
    values = models.JSONField(default=dict, blank=True)     # e.g. {"batch_size": 50}
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.CharField(max_length=150, blank=True)

    def __str__(self):
        return f"{self.stage} settings"
