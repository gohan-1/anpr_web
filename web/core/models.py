from django.db import models


class Route(models.TextChoices):
    PENDING = "pending", "Pending"
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
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="folders")
    path = models.CharField(max_length=500)
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
    processed_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.file_name
