import base64
import plotly.express as px
import plotly.graph_objects as go

def return_image(image_name):
	mime_type = image_name.split('.')[-1:][0].lower()        
	with open(image_name, "rb") as f:
	    content_bytes = f.read()
	content_b64encoded = base64.b64encode(content_bytes).decode()
	image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
	return image_string
