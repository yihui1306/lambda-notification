#!/usr/bin/env python
# coding: utf-8

# requirements
# !pip install ultralytics supervision
import requests
from ultralytics import YOLO
import supervision as sv
import cv2 as cv
import numpy as np
# import os
# import requests
from collections import Counter
def image_prediction(image_path, result_filename=None, save_dir = "./image_prediction_results", confidence=0.5, model="./model.pt"):
    """
    Function to display predictions of a pre-trained YOLO model on a given image.

    Parameters:
        image_path (str): Path to the image file. Can be a local path or a URL.
        result_path (str): If not None, this is the output filename.
        confidence (float): 0-1, only results over this value are saved.
        model (str): path to the model.
    """

    # Load YOLO model
    model = YOLO(model)
    class_dict = model.names

    # # Load image from local path
    # img = cv.imread(image_path)

    try:
        if image_path.startswith('http://') or image_path.startswith('https://'):
            response = requests.get(image_path)
            response.raise_for_status()
            image_array = np.asarray(bytearray(response.content), dtype=np.uint8)
            img = cv.imdecode(image_array, cv.IMREAD_COLOR)
        else:
            img = cv.imread(image_path)
    except Exception as e:
        print(f"Error loading image: {e}")
        return

    # Check if image was loaded successfully
    if img is None:
        print("Couldn't load the image! Please check the image path.")
        return

    # Get image dimensions
    h, w = img.shape[:2]

    # Calculate optimal thickness for boxes and text based on image resolution
    thickness = sv.calculate_optimal_line_thickness(resolution_wh=(w, h))
    text_scale = sv.calculate_optimal_text_scale(resolution_wh=(w, h))

    # Set up color palette for annotations
    color_palette = sv.ColorPalette.from_matplotlib('magma', 10)

    # Create box and label annotators
    box_annotator = sv.BoxAnnotator(thickness=thickness, color=color_palette)
    label_annotator = sv.LabelAnnotator(color=color_palette, text_scale=text_scale, 
                                        text_thickness=thickness, 
                                        text_position=sv.Position.TOP_LEFT)

    # Run the model on the image
    result = model(img)[0]

    # Convert YOLO result to Detections format
    detections = sv.Detections.from_ultralytics(result)
    counts = {}
    # Filter detections based on confidence threshold and check if any exist
    if detections.class_id is not None:
        detections = detections[(detections.confidence > confidence)]

        # Create labels for the detected objects
        labels = [f"{class_dict[cls_id]} {conf*100:.2f}%" for cls_id, conf in 
                  zip(detections.class_id, detections.confidence)]
    
        class_names = [class_dict[cls_id].lower() for cls_id in detections.class_id]
        counts = dict(Counter(class_names))
        # Annotate the image with boxes and labels
        box_annotator.annotate(img, detections=detections)
        label_annotator.annotate(img, detections=detections, labels=labels)

    # if result_filename:
    #     os.makedirs(save_dir, exist_ok=True)  # Ensure the save directory exists
    #     save_path = os.path.join(save_dir, result_filename)
    #     try:
    #         status = cv.imwrite(save_path, img)
    #         print(f"Image save status = {status}.")
    #     except Exception as e:
    #         print(f"Error saving image: {e}")
    # else:
    #     print("Filename is none, result is not saved.")

    return counts

# ## Video Detection
def video_prediction(video_path, result_filename=None, save_dir = "./video_prediction_results", confidence=0.5, model="./model.pt"):
    """
    Function to make predictions on video frames using a trained YOLO model and display the video with annotations.

    Parameters:
        video_path (str): Path to the video file.
        save_video (bool): If True, saves the video with annotations. Default is False.
        filename (str): The name of the output file where the video will be saved if save_video is True.
    """
    try:
        # Load video info and extract width, height, and frames per second (fps)
        video_info = sv.VideoInfo.from_video_path(video_path=video_path)
        w, h, fps = int(video_info.width), int(video_info.height), int(video_info.fps)

        # Calculate the optimal thickness for annotations and text scale based on video resolution
        thickness = sv.calculate_optimal_line_thickness(resolution_wh=video_info.resolution_wh)
        text_scale = sv.calculate_optimal_text_scale(resolution_wh=video_info.resolution_wh)

        # Initialize YOLO model, tracker, and color lookup for annotations
        box_annotator = sv.BoxAnnotator(thickness=thickness, color_lookup=sv.ColorLookup.TRACK)
        label_annotator = sv.LabelAnnotator(text_scale=text_scale, text_thickness=thickness, 
                                            text_position=sv.Position.TOP_LEFT,
                                            color_lookup=sv.ColorLookup.TRACK)

        model = YOLO(model)  # Load your custom-trained YOLO model
        tracker = sv.ByteTrack(frame_rate=fps)  # Initialize the tracker with the video's frame rate
        class_dict = model.names  # Get the class labels from the model

        # Directory to save the video with annotations, if required
        all_class_names = []
        # if result_filename:
        #     os.makedirs(save_dir, exist_ok=True)  # Ensure save directory exists
        #     save_path = os.path.join(save_dir, result_filename)
        #     out = cv.VideoWriter(save_path, cv.VideoWriter_fourcc(*"XVID"), fps, (w, h))  # Initialize video writer
        # else:
        #     print("Result filename is required to save the video file.")
        #     return
        
        # Capture the video from the given path
        cap = cv.VideoCapture(video_path)
        if not cap.isOpened():
            raise Exception("Error: couldn't open the video!")

        # Process the video frame by frame
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:  # End of the video
                break

            # Make predictions on the current frame using the YOLO model
            result = model(frame)[0]
            detections = sv.Detections.from_ultralytics(result)  # Convert model output to Detections format
            detections = tracker.update_with_detections(detections=detections)  # Track detected objects
  
            # Filter detections based on confidence
            if detections.tracker_id is not None:
                detections = detections[(detections.confidence > confidence)]  # Keep detections with confidence greater than a threashold
                frame_class_names = [class_dict[cls_id].lower() for cls_id in detections.class_id]
                all_class_names.extend(frame_class_names)
                # Generate labels for tracked objects
                labels_0 = [f"#{trk_id} {class_dict[cls_id]} {conf*100:.2f}%" 
                            for trk_id, cls_id, conf in zip(
                            detections.tracker_id, detections.class_id, detections.confidence)]

                labels_1 = [f"{class_dict[cls_id]} {conf*100:.2f}%" for cls_id, conf in zip(
                            detections.class_id, detections.confidence)]

                # Annotate the frame with bounding boxes and labels
                box_annotator.annotate(frame, detections=detections)
                label_annotator.annotate(frame, detections=detections, labels=labels_1)

            # Save the annotated frame to the output video file if save_video is True
            # if result_filename:
            #     out.write(frame)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Release resources
        cap.release()
        # if result_filename:
        #     out.release()
        print("Video processing complete, Released resources.")
    counts = dict(Counter(all_class_names))
    return counts

if __name__ == '__main__':
    print("predicting...")
    tags=image_prediction("https://media.birdweather.com/species/60/RockPigeon-original-dc261800fef40e2bff0e8edcdafe13a5.jpg", result_filename="crows_result1.jpg")
    print("Detected tags:", tags)

  
    # tags=video_prediction("./test_videos/crows.mp4",result_filename='crows_detected.mp4')
    # print("Detected tags video:", tags)
    # tags=video_prediction("./test_videos/kingfisher.mp4",result_filename='kingfisher_detected.mp4')
    # print("Detected tags video:", tags)