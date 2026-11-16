# """Local image quality check with OpenCV. Runs before the API to catch blurry/dark/tiny images."""
# from __future__ import annotations

# from pathlib import Path

# import cv2
# import numpy as np


# def assess_quality(path: Path, cfg: dict) -> dict | None:
#     """Returns quality metrics, or None if the image can't be read (corrupt)."""
#     data = np.fromfile(str(path), dtype=np.uint8)
#     img = cv2.imdecode(data, cv2.IMREAD_COLOR) if data.size else None
#     if img is None:
#         return None

#     gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
#     h, w = gray.shape

#     blur_var = float(cv2.Laplacian(gray, cv2.CV_64F).var())
#     blur_score = min(blur_var / cfg["blur_reference"], 1.0)

#     brightness = float(gray.mean())
#     brightness_score = max(0.0, 1.0 - abs(brightness - 128) / 128)

#     resolution_score = min(min(h, w) / cfg["min_side_px"], 1.0)

#     wts = cfg["weights"]
#     quality = (wts["blur"] * blur_score + wts["brightness"] * brightness_score
#                + wts["resolution"] * resolution_score)

#     return {
#         "quality": round(quality, 4),
#         "blur_variance": round(blur_var, 2),
#         "brightness": round(brightness, 2),
#         "width": w,
#         "height": h,
#     }
