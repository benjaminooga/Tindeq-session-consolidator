'''
Assumes info.csv structure: date,tag,comment,unit,reps,work dur.,pause btw. reps,sets,pause btw. sets,type

Uses the save name of your session as the exercise name

Outputs:
    1. CSV file containing all exported sessions
    2. Chart showing the max Peaks and average of Averages per day

'''
from zipfile import ZipFile
import argparse, sys
import pandas as pd
import AccountKeys

PATH = AccountKeys.KEYS['TINDEQ_PATH']

def consolidator(zipfilename):
    ''' '''
    tindeq_df = pd.DataFrame()

    # Unzip main zip, process sessions within
    with ZipFile(zipfilename, 'r') as outer_zip:
        for csvzip in outer_zip.namelist():
            with outer_zip.open(csvzip) as inner_zip_file:
                info = []   # info.csv record
                
                # Process session zip file
                with ZipFile(inner_zip_file, 'r') as inner_zip:
                    # Read sesson information from info.csv
                    with inner_zip.open("info.csv") as file:
                        # Pandas is not handling the date properly, so...
                        contents = file.read().decode('utf-8')
                        headers = contents.split('\n')[0].split(',')
                        info = contents.split('\n')[1].split(',')
                        
                        if headers[0] != 'date' or headers[7] != 'sets':
                            print(f'[-] ERROR: Information file not in expected format ({csvzip})')
                            next

                    for set in range(1, int(info[7]) + 1):
                        with inner_zip.open("data_set_" + str(set) + ".csv") as file:
                            dates = [] # info[0]
                            times = [] # info[0]
                            exercises = [] # info[1] (tag)
                            sets = [] # info[7]
                            hands = []
                            reps = []
                            measures = []
                            values = []
                            dur_works = [] # info[5]
                            dur_pauses = [] # info[6]
                            
                            content = file.read().decode('utf-8')

                            for row in content.split('\n'):
                                # Process only Avg/Peak rows; skipping detailed measurements
                                if row.startswith('Avg ') or row.startswith('Peak '):
                                    records = row.split(',')
                                    measure, hand = records[0].split(' ')
                                    
                                    for rep in range(1, len(records) - 1):
                                        dates.append(pd.to_datetime(info[0], format='%Y-%d-%m %H:%M:%S'))
                                        times.append(pd.to_datetime(info[0], format='%Y-%d-%m %H:%M:%S'))
                                        exercises.append(info[1])
                                        dur_works.append(info[5])
                                        dur_pauses.append(info[6])
                                        sets.append(set)
                                        hands.append(hand)
                                        reps.append(rep)
                                        measures.append(measure)
                                        values.append(records[rep])

                            tindeq_df = pd.concat([tindeq_df, pd.DataFrame({
                                'Date': dates, 
                                'Time': times, 
                                'Exercise': exercises,
                                'DurationWork': dur_works,
                                'DurationPause': dur_pauses,
                                'Set': sets, 
                                'Hand': hands, 
                                'Rep': reps, 
                                'Measure': measures, 
                                'Value': values})], ignore_index=True)

    tindeq_df['Date'] = tindeq_df['Date'].dt.date
    tindeq_df['Rep'] = tindeq_df['Set'].astype(int)
    tindeq_df['Rep'] = tindeq_df['Rep'].astype(int)

    # Remove empty measurements
    tindeq_df = tindeq_df[tindeq_df['Value'] != '']
    tindeq_df['Value'] = tindeq_df['Value'].astype(float)

    # Determine the number of sets and adjust Set
    tindeq_df['Set'] += (tindeq_df.sort_values('Time').groupby('Date')['Time'].transform(lambda x: (x.diff() > '00:01:00').cumsum()))

    return tindeq_df


def main():
    # zipfilename = PATH + 'tindeq.zip'
    zipfilename = 'tindeq0331.zip'

    tindeq_df = consolidator(PATH + zipfilename)

    # TODO: Output as CSV, JSON, both
    tindeq_df.to_csv(PATH + zipfilename.split('.')[0] + '.csv')
    
    pretty_json = tindeq_df.T.to_json(PATH + zipfilename.split('.')[0] + '.json', indent=4)
    
    # with open(PATH + zipfilename.split('.')[0] + '.json', 'w') as jfile:
    #     jfile.write(pretty_json)

if __name__ == "__main__":
    main()