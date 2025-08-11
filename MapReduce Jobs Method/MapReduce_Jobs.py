import csv
import re
from io import StringIO

from mrjob.job import MRJob
from mrjob.step import MRStep

# Define a class for our MapReduce job, inheriting from MRJob
class MRCommonLyrics(MRJob):

    # 1. Define the list of common lyrics to search for.
    #    It's converted to a set for efficient lookup and all words are lowercase
    #    to ensure case-insensitive matching.
    COMMON_LYRICS_LIST = {
        "we", "yeah", "hell", "die", "u", "like", "breathe", "it", "ya",
        "you", "up", "get", "thang", "love", "fire", "don't", "rock",
        "on", "woman", "disco", "music", "dancin'", "baby", "twist",
        "little", "lonely", "christmas", "penny", "mambo", "three"
    }

    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)
        ]

    def mapper_get_words(self, _, line):
      
        try:
            
            reader = csv.reader(StringIO(line))
            row_data = next(reader)

           
            if len(row_data) > 5 and 'artist_name' in row_data[1] and 'lyrics' in row_data[5]:
                
                self.increment_counter('Skipped Lines', 'Header Row', 1)
                return

           
            if len(row_data) > 5:
                lyrics_text = row_data[5]

                
                if not lyrics_text or not str(lyrics_text).strip():
                    self.increment_counter('Skipped Lines', 'Empty Lyrics', 1)
                    return

                
                words = re.findall(r"\b[\w']+\b", str(lyrics_text).lower())

                
                for word in words:
                    if word in self.COMMON_LYRICS_LIST:
                        yield word, 1
            else:
                
                self.increment_counter('Skipped Lines', 'Not Enough Columns', 1)

        except Exception as e:
            
            self.increment_counter('Errors', f'Parsing Error: {type(e).__name__}', 1)
            

    def combiner_count_words(self, word, counts):
       
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        
        yield word, sum(counts)

def run_mrjob_script_on_csv(file_path):
   
    try:
        
        mr_job_instance = MRCommonLyrics(args=[file_path])

        print(f"Starting MRJob execution on '{file_path}'...")
        processed_results = []
        
       
        for key, value in mr_job_instance.run_job():
            processed_results.append((key, value))

        # Sort the final counts in descending order for better readability.
        sorted_counts = sorted(processed_results, key=lambda item: item[1], reverse=True)

        print("\nCommon Lyric Counts (MRJob Results):")
        if not sorted_counts:
            print("No common lyrics from the provided list were found in the dataset.")
        else:
            for lyric, count in sorted_counts:
                print(f"'{lyric}': {count}")
        
        

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found. Please ensure it's in the correct directory.")
    except Exception as e:
        print(f"An unexpected error occurred during MRJob execution: {e}")


run_mrjob_script_on_csv('SongLyrics.csv')
