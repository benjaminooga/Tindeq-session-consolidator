'''
Assumes info.csv structure: date,tag,comment,unit,reps,work dur.,pause btw. reps,sets,pause btw. sets,type

Uses the saved name of your session as the exercise name. Details of the set (reps, work, and rest) are saved with each record. 
Note that reset between sets is not included.

Whether your saved sessions have one or multiple sets, the script also looks for follow-up sets, so your sets in the output may differ
from within the Tindeq app if you have a mix of sessions on a day. This is so analysis is easier in that you don't always have to look 
at the time and compare against other records.

Inputs:
    Assumes that the zip file contains zipped sessions. Only tested with Repeaters data.
Outputs:
    1. CSV file containing all exported sessions
    2. JSON file containing all exported sessions
'''
from zipfile import ZipFile
import argparse
import pandas as pd
import AccountKeys
import os

PATH = AccountKeys.KEYS['TINDEQ_PATH']

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTIONS]",
        description='-'*80 + '\nTindeq Session Consolidator\n' + '-'*80 + '\n'
    )
    parser.add_argument("filename", type=str, help="Zip file to convert")
    parser.add_argument("-c", "--csv", action='store_true', help="Ouput CSV (default)")
    parser.add_argument("-j", "--json", action='store_true', help="Output JSON")
    parser.add_argument("-v", "--version", action="version", version=f"{parser.prog} version 1.0.0")
    return parser

def check_path(filename_or_path):
    if os.path.isabs(filename_or_path):
        # Indicates full path
        return 1

    # Just a file
    return 0

def consolidator(zipfilename):
    ''' Take a zip file and return a dataframe of all nested zip contents '''
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
    parser = init_argparse()
    args = parser.parse_args()
    
    if check_path(args.filename) == 0:
        output_dir = os.getcwd() + '/'
    else:
        output_dir = ''
    
    if args.filename:
        print(f"[+] INFO: Consolidating '{output_dir + args.filename}'")
    else:
        print(f"[+] INFO: File not specified, looking for 'tindeq.zip'")
        args.filename = 'tindeq.zip'

    zipfilename = args.filename

    tindeq_df = consolidator(zipfilename)

    # Write the output
    if args.csv or (args.csv == False and args.json == False):
        print(f"[+] INFO: Writing CSV '{output_dir + zipfilename.split('.')[0] + '.csv'}'")
        tindeq_df.to_csv(output_dir + zipfilename.split('.')[0] + '.csv')
    
    if args.json:
        print(f"[+] INFO: Writing JSON '{output_dir + zipfilename.split('.')[0] + '.json'}'")
        pretty_json = tindeq_df.T.to_json(output_dir + zipfilename.split('.')[0] + '.json', indent=4)

if __name__ == "__main__":
    main()