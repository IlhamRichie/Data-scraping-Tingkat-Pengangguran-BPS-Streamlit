import streamlit as st
import cv2
import numpy as np
import tensorflow as tf
from PIL import Image
import av
from streamlit_webrtc import webrtc_streamer, WebRtcMode, RTCConfiguration

# Konfigurasi WebRTC
RTC_CONFIGURATION = RTCConfiguration(
    {"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]}
)

# Judul aplikasi
st.title("Realtime Emotion Detection with Face Detection")

# Load Haar Cascade untuk deteksi wajah
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

# Upload model dan label
uploaded_model = st.sidebar.file_uploader("Upload model .tflite", type=["tflite"])
uploaded_labels = st.sidebar.file_uploader("Upload labels.txt", type=["txt"])

def load_labels(label_path):
    with open(label_path, 'r') as f:
        labels = {int(line.split()[0]): line.split()[1] for line in f.readlines()}
    return labels

def load_tflite_model(model_path):
    interpreter = tf.lite.Interpreter(model_path=model_path)
    interpreter.allocate_tensors()
    return interpreter

if 'interpreter' not in st.session_state:
    st.session_state.interpreter = None
if 'labels' not in st.session_state:
    st.session_state.labels = None

if uploaded_model and uploaded_labels:
    with open("temp_model.tflite", "wb") as f:
        f.write(uploaded_model.getbuffer())
    with open("temp_labels.txt", "wb") as f:
        f.write(uploaded_labels.getbuffer())
    
    try:
        st.session_state.interpreter = load_tflite_model("temp_model.tflite")
        st.session_state.labels = load_labels("temp_labels.txt")
        st.sidebar.success("Model loaded successfully!")
    except Exception as e:
        st.sidebar.error(f"Error: {str(e)}")

def video_frame_callback(frame):
    if st.session_state.interpreter is None or st.session_state.labels is None:
        return frame
    
    img = frame.to_ndarray(format="bgr24")
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Deteksi wajah
    faces = face_cascade.detectMultiScale(gray, 1.3, 5)
    
    for (x, y, w, h) in faces:
        # Gambar bounding box di sekitar wajah
        cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)
        
        # Ekstrak ROI (Region of Interest) wajah
        face_roi = gray[y:y+h, x:x+w]
        
        # Preprocess untuk model emosi
        input_details = st.session_state.interpreter.get_input_details()
        input_shape = input_details[0]['shape']
        input_img = cv2.resize(face_roi, (input_shape[1], input_shape[2]))
        
        if input_shape[3] == 1:
            input_img = np.expand_dims(input_img, axis=-1)
        
        input_data = np.expand_dims(input_img, axis=0)
        
        if input_details[0]['dtype'] == np.float32:
            input_data = (np.float32(input_data) / 255.0)
        
        # Prediksi emosi
        st.session_state.interpreter.set_tensor(input_details[0]['index'], input_data)
        st.session_state.interpreter.invoke()
        output_data = st.session_state.interpreter.get_tensor(
            st.session_state.interpreter.get_output_details()[0]['index'])
        
        predicted_class = np.argmax(output_data[0])
        confidence = np.max(output_data[0])
        label = st.session_state.labels[predicted_class]
        
        # Tampilkan label emosi di atas bounding box
        cv2.putText(img, f"{label} ({confidence:.2f})", (x, y-10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)
    
    return av.VideoFrame.from_ndarray(img, format="bgr24")

if st.session_state.interpreter and st.session_state.labels:
    st.header("Realtime Face Emotion Detection")
    webrtc_ctx = webrtc_streamer(
        key="emotion-detection",
        mode=WebRtcMode.SENDRECV,
        rtc_configuration=RTC_CONFIGURATION,
        video_frame_callback=video_frame_callback,
        media_stream_constraints={"video": True, "audio": False},
        async_processing=True,
    )
else:
    st.warning("Please upload model and label files first")