import csv

def count_total_common_lyrics(file_path, common_lyrics_list):
    
    # Initialize a dictionary to store the counts for each common lyric
    common_lyrics_counts = {lyric: 0 for lyric in common_lyrics_list}

    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)

            # Skip the header row
            header = next(reader, None)
            if header is None:
                print("The CSV file is empty or has no header.")
                return

            # Find the index of the 'lyrics' column
            try:
                lyrics_col_index = header.index('lyrics')
            except ValueError:
                print("Error: 'lyrics' column not found in the CSV file.")
                return

            # Iterate through each row (song) in the CSV
            for row in reader:
                if len(row) > lyrics_col_index:
                    lyrics_text = row[lyrics_col_index].lower()
                    
                    # Count occurrences of each common lyric in the current song
                    for common_lyric in common_lyrics_list:
                        common_lyrics_counts[common_lyric] += lyrics_text.count(common_lyric.lower())
                else:
                    print(f"Skipping row due to insufficient columns: {row}")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

    # Sort the dictionary by count in descending order
    sorted_lyrics = sorted(common_lyrics_counts.items(), key=lambda item: item[1], reverse=True)

    # Print the sorted results
    print("Total count of common lyrics across all songs (in descending order):")
    for lyric, count in sorted_lyrics:
        print(f"'{lyric}': {count}")


# Define the list of common lyrics provided by the user
user_common_lyrics = [
    "We", "Yeah", "Hell", "Die", "U", "Like", "Breathe", "It", "Ya", "U", "You", "Up", "Get", "Thang", "Love", "Fire", "Don’t", "Rock", "On", "Woman", "Disco", "Rock", "Music", "Dancin’", "Baby", "Twist", "Little", "Lonely", "Christmas", "Penny", "Mambo", "Three"
]

# Specify the path to your CSV file
file_path = 'tcc_ceds_music.csv'

# Call the function to perform the task

count_total_common_lyrics(file_path, user_common_lyrics)
