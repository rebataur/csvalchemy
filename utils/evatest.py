# Connect to EvaDB for running AI queries
import evadb
cursor = evadb.connect().cursor()

# Load the MNIST video into EvaDB
# cursor.load("C:\\3Projects\\rapidiam\\utils\\mnist.mp4", "MNISTVid", format="video").df()

# We now construct an AI pipeline to run the image classifier
# over all the digit images in the video
# Each frame in the loaded MNIST video contains a digit

# Connect to the table with the loaded video
query = cursor.table("MNISTVid")

# Run the model on a subset of frames
# Here, id refers to the frame id
query = query.filter("id = 30 OR id = 50 OR id = 70")
response = query.df()
print(response)
# We are retrieving the frame "data" and
# the output of the Image Classification function on the data
# query = query.select("data, MnistImageClassifier(data).label")

# # EvaDB uses a lazy query construction technique to improve performance
# # Only calling query.df() will run the query
# response = query.df()