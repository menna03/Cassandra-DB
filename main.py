import base64
import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from PIL import Image
from io import BytesIO
import os

def query_and_display_movies_name(search_term, output_folder):
    # Load credentials from JSON file
    with open("ahmadgadalla02@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Connection setup using secure connect bundle
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-ahmad.zip'
    }

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

    # Create the cluster and session
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    try:
        # Keyspace to use
        keyspace_name = "movies"

        # Use the keyspace
        session.set_keyspace(keyspace_name)

        # Use the LIKE operator to search for movies by director or actor
        query = f"SELECT id, movie_cast,movie_poster,name FROM movie WHERE name = '{search_term}' ALLOW FILTERING;"



        # Execute the query
        rows = session.execute(query)

        # Create the output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # Display and save the results
        for row in rows:
            print(f"Movie ID: {row.id}, Name: {row.name}, Movie Cast: {row.movie_cast}\n")
            print(f"___________________")

            # Save the image to the output folder
            if row.movie_poster:
                image_data = row.movie_poster
                image = Image.open(BytesIO(image_data))
                image_path = os.path.join(output_folder, f"{row.id}_{row.name}_poster.jpg")
                image.save(image_path)
                print(f"Image saved to: {image_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cluster.shutdown()

def query_and_display_movies_by_director_or_actor(search_term, output_folder):
    # Load credentials from JSON file
    with open("ahmadgadalla02@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Connection setup using secure connect bundle
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-ahmad.zip'
    }

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

    # Create the cluster and session
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    try:
        # Keyspace to use
        # Keyspace to use
        keyspace_name = "movies"

        # Use the keyspace
        session.set_keyspace(keyspace_name)

        # Use the LIKE operator to search for movies by director or actor
        query = f"SELECT * FROM movie ALLOW FILTERING;"

        # Execute the query
        rows = session.execute(query)

        # Create the output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # Display and save the results
        for row in rows:
            movie_id = row.id
            movie_name = row.name
            movie_cast = row.movie_cast
            movie_cast = str(movie_cast)
            if search_term in movie_cast:
                movie_poster = row.movie_poster
                print(f"Movie ID: {movie_id}, Name: {movie_name}, Cast:{movie_cast}")
                image_data = row.movie_poster
                image = Image.open(BytesIO(image_data))
                image_path = os.path.join(output_folder, f"{row.id}_{row.name}_poster.jpg")
                image.save(image_path)
                print(f"Image saved to: {image_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cluster.shutdown()

def update_movie_actors(movie_id, new_actor):
    # Load credentials from JSON file
    with open("ahmadgadalla02@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Connection setup using secure connect bundle
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-ahmad.zip'
    }

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

    # Create the cluster and session
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    try:
        # Keyspace to use
        keyspace_name = "movies"

        # Use the keyspace
        session.set_keyspace(keyspace_name)

        # Fetch the existing actors string for the movie
        select_query = f"SELECT movie_cast FROM movie WHERE id = {movie_id};"
        result = session.execute(select_query).one()

        # Extract the existing actors string
        existing_actors_str = result.movie_cast.get('actors', '') if result.movie_cast else ''

        # Append the new actor to the existing actors string
        updated_actors_str = f"{existing_actors_str}, {new_actor}" if existing_actors_str else new_actor

        # Update the map with the modified string
        update_query = f"UPDATE movie SET movie_cast = {{ 'actors': '{updated_actors_str}' }} WHERE id = {movie_id};"
        session.execute(update_query)

        print(f"Updated movie {movie_id} with {new_actor} added to the actor list.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cluster.shutdown()


def connect_and_update_blob(folder_path):
    # Load credentials from JSON file
    with open("ahmadgadalla02@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Connection setup using secure connect bundle
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-ahmad.zip'
    }

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

    # Create the cluster and session
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    # Keyspace to use
    keyspace_name = "movies"

    # Use the keyspace
    session.set_keyspace(keyspace_name)

    # Get all files in the folder
    files = os.listdir(folder_path)[:3]  # Select the first three files in the folder

    # Update movie-poster column for the three inserted rows
    for idx, file_name in enumerate(files, 1):
        # Read the image as binary data
        with open(os.path.join(folder_path, file_name), 'rb') as image_file:
            image_data = image_file.read()

        # Update the movie-poster column for the specific row
        query = f"UPDATE movie SET movie_poster = %s WHERE id = {idx}"
        session.execute(query, (image_data,))  # Assuming 'movie' table and 'id' column

    cluster.shutdown()


def update_ttl(movie_id, ttl):
    # Load credentials from JSON file
    with open("ahmadgadalla02@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Connection setup using secure connect bundle
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-ahmad.zip'
    }

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

    # Create the cluster and session
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    # Keyspace to use
    keyspace_name = "movies"
    session.set_keyspace(keyspace_name)

    # Step 1: Read the existing row with the desired movie_id
    query = "SELECT * FROM movie WHERE id = %s"
    result = session.execute(query, (movie_id,))
    for row in result:
        movie_details = row

    if not movie_details:
        print(f"No movie found with ID: {movie_id}")
        return
    try:
        # Step 2: Delete the old row
        delete_query = "DELETE FROM movie WHERE id = %s"
        session.execute(delete_query, (movie_id,))

        # Step 3: Insert the row back with the new TTL value
        insert_query = """
        INSERT INTO movie (id, name, movie_cast, movie_poster) 
        VALUES (%s, %s, %s, %s) USING TTL %s
        """
        session.execute(insert_query, (movie_details.id, movie_details.name, movie_details.movie_cast, movie_details.movie_poster, ttl))
        print(f"TTL updated successfully for movie with ID: {movie_id}")

    except Exception as e:
        print(f"An error occurred: {e}")



    # Close the session and cluster connection
    session.shutdown()
    cluster.shutdown()

import sys

def main_menu():
    print("Welcome to the Movie Database!")

    while True:
        print("\nChoose an option:")
        print("1. Search and display movies by director or actor")
        print("2. Search and display movies by name")
        print("3. Update movie actors")
        print("4. Connect and update blob")
        print("5. Update TTL for a movie")
        print("6. Exit")

        choice = input("Enter the number of your choice: ")

        if choice == "1":
            search_term = input("Enter director or actor name: ")
            output_folder = input("Enter output folder: ")
            query_and_display_movies_by_director_or_actor(search_term, output_folder)

        elif choice == "2":
            search_term = input("Enter movie name: ")
            output_folder = input("Enter output folder: ")
            query_and_display_movies_name(search_term, output_folder)

        elif choice == "3":
            movie_id = input("Enter movie ID: ")
            new_actor = input("Enter new actor's name: ")
            update_movie_actors(int(movie_id), new_actor)

        elif choice == "4":
            folder_path = input("Enter folder path: ")
            connect_and_update_blob(folder_path)

        elif choice == "5":
            movie_id = input("Enter movie ID: ")
            ttl = input("Enter new TTL (time to live) in seconds: ")
            update_ttl(int(movie_id), int(ttl))

        elif choice == "6":
            print("Exiting the Movie Database. Goodbye!")
            break

        else:
            print("Invalid choice. Please enter a valid number.")


main_menu()


#update_movie_actors(1,'Ahmad Gadalla')
# Replace 'your_search_term' and 'your_output_folder' with actual values
#query_and_display_movies_by_director_or_actor('Radwa', 'output_folder')

#query_and_display_movies_by_director_or_actor('John Goodman', 'output_folder')

# Replace 'your_search_term' and 'your_output_folder' with actual values
#query_and_display_movies_name('Charlie and the chocolate factory', 'output_folder')
# update_ttl(1, 3)

#connect_and_update_blob('p')