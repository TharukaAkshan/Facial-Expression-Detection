#Genral libraries
import os
import pandas as pd
import numpy as np
import Utils as utils
from PIL import Image

#streamlit libraries
import streamlit as st

#NNN libraries
#import cv2
import tensorflow as tf
from keras.models import load_model


configs = utils.load_config(r'D:\3. ICBT Degree\Module 05 Final Dissertation Project New\Music Therapy Streamlit App\config\configurations.yml')
model = load_model(configs['data']['model_path'],compile=False)
class_dict = {0:'Angry',1:'Happy',2:'Neutral',3:'Sad'}


def classifer(img_tensor):
    """
    Predicts the emotion label of image tensor

    Parameters
    ----------
    img_tensor : tf.tensor

    returns
    -------
    class_dict[int(label)]: string
    the predicted emotion label
    """
    result = model.predict_on_batch(img_tensor)
    label = np.array(result).argmax()
    return class_dict[int(label)]


def fetch_song_list(emo_class):
    """
    Fetches the song list realted to the
    predicted label

    Parameters
    ----------
    emo_class : string

    returns
    -------
    song_list_df: pandas dataframe
    fetched song list dataframe
    """
    emo_class_folder = os.path.join(configs['data']['Songs'],emo_class)
    song_excel_path = os.path.join(emo_class_folder,os.listdir(emo_class_folder)[0])
    song_list_df = pd.read_excel(song_excel_path)
    song_list_df.drop('ID',axis= 1,inplace=True)
    return song_list_df


def preprocess(image):
    """
    Preproccess the captured image
    suitable for the model

    Parameters
    ----------
    image : BytesIO

    returns
    -------
    image: tf.tensor
    preprocessed image tensor
    """
    image = tf.keras.utils.img_to_array( image )
    image = tf.image.resize(image, [48,48], method='nearest')
    image = tf.image.rgb_to_grayscale(image)
    image = tf.reshape(image,(1, 48, 48, 1))
    return image


def main():
    """
    the  main  function of
    streamlit App

    Parameters
    ----------
    None

    returns
    -------
    None
    """
    st.markdown("<h1 style='text-align: center; color: white;'>Music Therapy</h1>", unsafe_allow_html=True)
    with st.sidebar:
        img_file_buffer = st.camera_input("Scan Face",label_visibility="hidden")
    if img_file_buffer is not None:
        bytes_data = img_file_buffer.getvalue()
        img_tensor = tf.io.decode_image(bytes_data, channels=3)
        img_tensor = preprocess(img_tensor)
        emo_class = classifer(img_tensor)
        if emo_class == 'Angry':
            st.subheader(f"Your Mood is :red[{emo_class}]")
            song_list_df = fetch_song_list(emo_class)
            st.dataframe(song_list_df)
        elif emo_class == 'Happy':
            st.subheader(f"Your Mood is :green[{emo_class}]")
            song_list_df = fetch_song_list(emo_class)
            st.dataframe(song_list_df)
        elif emo_class == 'Neutral':
            st.subheader(f"Your Mood is {emo_class}")
            song_list_df = fetch_song_list(emo_class)
            st.dataframe(song_list_df)
        elif emo_class == 'Sad':
            st.subheader(f"Your Mood is :red[{emo_class}]")
            song_list_df = fetch_song_list(emo_class)
            st.dataframe(song_list_df)

if "main" in __name__:
    main()