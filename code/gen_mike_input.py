import getopt
import json
import sys
import traceback
import os
import pandas as pd
from datetime import datetime, timedelta
from db_layer import CurwSimAdapter
from functools import reduce


def get_mean_rain(db_adapter, available_station_list, ts_start, ts_end):
    print()


def get_individual_rain(db_adapter, available_station_list, ts_start, ts_end):
    df_list = []
    for [hash_id, station] in available_station_list:
        tms_df = db_adapter.get_timeseries_by_id(hash_id, ts_start, ts_end)
        if tms_df is not None:
            tms_df.rename(columns={'value': station}, inplace=True)
            df_list.append(tms_df)
    if len(df_list) > 1:
        merged_df = reduce(lambda left, right: pd.merge(left, right, on='Times'), df_list)
        print('merged_df : ', merged_df)
        return merged_df


def create_hybrid_mike_input(mode, output_path, run_date, forward, backward):
    print('create_hybrid_mike_input|output_path: ', output_path)
    try:
        run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
        ts_end = (run_datetime + timedelta(days=forward)).strftime('%Y-%m-%d %H:%M:%S')
        ts_start = (run_datetime - timedelta(days=backward)).strftime('%Y-%m-%d %H:%M:%S')
        print('[ts_end, ts_start] : ', [ts_end, ts_start])
        available_station_list = db_adapter.get_available_stations(ts_start)
        if len(available_station_list) > 0:
            print('available_station_list : ', available_station_list)
            if mode == 'mean':
                output_file = os.path.join(output_path, 'mike_input_mean.csv')
            else:
                output_file = os.path.join(output_path, 'mike_input_individual.csv')
                rain_df = get_individual_rain(db_adapter, available_station_list, ts_start, ts_end)
                rain_df.to_csv(output_file, header=True)
        else:
            print('no available stations.')
    except Exception as e:
        print('Mike generation error|Exception:', str(e))
        traceback.print_exc()


if __name__ == "__main__":
    dir_path = '/mnt/disks/curwsl_nfs/mike'
    run_date = '2019-05-28'
    run_time = '14:00:00'
    forward = '2'
    backward = '3'
    mode = 'individual'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:f:b:m:", [
            "help", "date=", "time=", "forward=", "backward=", "mode="
        ])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            sys.exit()
        elif opt in ("-d", "--date"):
            run_date = arg  # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg  # 16:00:00
        elif opt in ("-f", "--forward"):
            forward = int(arg)
        elif opt in ("-b", "--backward"):
            backward = int(arg)
        elif opt in ("-m", "--mode"):
            mode = int(arg)
    config_path = os.path.join('/home/uwcc-admin/mike21/mike_input/code', 'config.json')
    print('config_path : ', config_path)
    with open(config_path) as json_file:
        config = json.load(json_file)
        dir_path = config['dir_path']
        if 'curw_sim_db_config' in config:
            curw_sim_db_config = config['curw_sim_db_config']
        else:
            exit(2)
        db_adapter = CurwSimAdapter(curw_sim_db_config['user'], curw_sim_db_config['password'],
                                    curw_sim_db_config['host'], curw_sim_db_config['db'])
        output_path = os.path.join(dir_path, run_date, run_time)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        output_file = os.path.join(output_path, 'mike_input.csv')
        create_hybrid_mike_input(mode, output_file, run_date, forward, backward)
        try:
            if db_adapter is not None:
                db_adapter.close_connection()
        except Exception as ex:
            print(str(ex))

