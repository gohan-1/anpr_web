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
    """One worker process that takes folders from the queue and calls the plate API.

    The supervisor reads this table: desired_running=True means "this bot should be
    running", and the bot itself reports its real state with a heartbeat.
    """

    class State(models.TextChoices):
        STOPPED = "stopped", "Stopped"
        STARTING = "starting", "Starting"
        RUNNING = "running", "Running"
        STOPPING = "stopping", "Stopping"
        CRASHED = "crashed", "Crashed"

    name = models.SlugField(max_length=50, unique=True)   # letters, numbers, - and _ only
    api_username = models.CharField(max_length=150)
    api_password_encrypted = models.TextField()            # never stored as plain text
    concurrency = models.PositiveSmallIntegerField(default=4)   # folders worked on at once
    rate_limit = models.FloatField(default=0)              # API calls per second, 0 = no limit
    input_dir = models.CharField(max_length=500, blank=True)  # folder this bot watches; "" = shared default queue

    # What we WANT (set from React)
    desired_running = models.BooleanField(default=False)
    delete_requested = models.BooleanField(default=False)

    # What IS happening (reported by the bot / supervisor)
    state = models.CharField(max_length=10, choices=State.choices, default=State.STOPPED)
    pid = models.PositiveIntegerField(null=True, blank=True)
    host = models.CharField(max_length=255, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    last_heartbeat = models.DateTimeField(null=True, blank=True)
    current_task = models.TextField(blank=True)            # folders being worked on right now
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


class WorkItem(models.Model):
    """One folder waiting in (or done with) the queue."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        IN_PROGRESS = "in_progress", "In progress"
        DONE = "done", "Done"
        FAILED = "failed", "Failed"

    path = models.CharField(max_length=500, unique=True)   # unique: a folder is queued only once
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.PENDING, db_index=True)
    bot = models.ForeignKey(Bot, null=True, blank=True, on_delete=models.SET_NULL, related_name="items")
    # Only this bot may take the folder. Empty = any bot without its own input folder.
    assigned_bot = models.ForeignKey(Bot, null=True, blank=True, on_delete=models.SET_NULL,
                                     related_name="assigned_items")
    bot_name = models.CharField(max_length=50, blank=True)    # kept even if the bot is deleted
    attempts = models.PositiveSmallIntegerField(default=0)
    claimed_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_error = models.TextField(blank=True)
    folder = models.OneToOneField(Folder, null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.path} ({self.status})"


# ============================================================================ intake (reader service)
class IntakeFolder(models.Model):
    """One vehicle folder found by the reader service, waiting to be processed.

    The reader only REGISTERS folders. Later stages (the Prefect flow) pick them up
    in priority order: lower number first, so low-accuracy sources go before high.
    """

    class Status(models.TextChoices):
        NEW = "new", "New"                      # registered, waiting for the pipeline
        PROCESSING = "processing", "Processing"
        DONE = "done", "Done"
        FAILED = "failed", "Failed"

    class Accuracy(models.TextChoices):
        LOW = "low", "Low accuracy"
        HIGH = "high", "High accuracy"

    path = models.CharField(max_length=500, unique=True)    # unique: registered once, even with many readers
    name = models.CharField(max_length=255, db_index=True)
    source = models.CharField(max_length=100, db_index=True)  # which configured source it came from
    accuracy = models.CharField(max_length=4, choices=Accuracy.choices)
    priority = models.PositiveSmallIntegerField(db_index=True)
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.NEW, db_index=True)
    image_count = models.PositiveIntegerField(default=0)
    duplicate_count = models.PositiveIntegerField(default=0)
    total_bytes = models.BigIntegerField(default=0)
    folder_modified_at = models.DateTimeField(null=True)
    discovered_at = models.DateTimeField(auto_now_add=True, db_index=True)
    discovered_by = models.CharField(max_length=100, blank=True)   # which reader copy registered it

    class Meta:
        # The order the pipeline should take folders in: priority first, then oldest first.
        indexes = [models.Index(fields=["status", "priority", "discovered_at"], name="intake_next_idx")]

    def __str__(self):
        return f"{self.name} ({self.accuracy}, {self.status})"


class IntakeImage(models.Model):
    """One image file inside an IntakeFolder, with what the reader learned about it."""
    folder = models.ForeignKey(IntakeFolder, on_delete=models.CASCADE, related_name="images")
    file_name = models.CharField(max_length=255)
    path = models.CharField(max_length=500)
    size_bytes = models.BigIntegerField()
    sha256 = models.CharField(max_length=64, db_index=True)   # fingerprint: same content = same hash
    is_duplicate = models.BooleanField(default=False)        # same content as another image in this folder
    modified_at = models.DateTimeField()

    class Meta:
        ordering = ["file_name"]