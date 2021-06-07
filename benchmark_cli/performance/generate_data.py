import subprocess, glob, shutil

from pathlib import Path

from benchmark_cli.performance.constants import *


def generate_data(stream_count : int):
    updates = (stream_count + 1) * 2

    # generate database bulk load
    # call dbgen with scale factor
    # subprocess.run([f'{DBGEN_DIR}/dbgen',
    #                 '-vf',
    #                 '-s', f'{SCALE_FACTOR}'],
    #                cwd=DBGEN_DIR)
    # # create folder for data if not exist
    # Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    # # move generated data to data folder
    # for file in glob.glob(f'{DBGEN_DIR}/*.tbl'):
    #     shutil.move(file, f'{DATA_DIR}/{os.path.basename(file)}')

    # generate updates
    # call dbgen with scale factor and updates
    subprocess.run([f'{DBGEN_DIR}/dbgen',
                    '-vf',
                    '-U', f'{updates}',
                    '-s', f'{SCALE_FACTOR}'],
                   cwd=DBGEN_DIR)
    # create folder for refresh data
    Path(REFRESH_DATA_DIR).mkdir(parents=True, exist_ok=True)

    # move generated updates to refresh data folder
    for file in [f for f_ in [glob.glob(f'{DBGEN_DIR}/{type}') for type in ('*.tbl.u*', 'delete.*')] for f in f_]:
        shutil.move(file, f'{REFRESH_DATA_DIR}/{os.path.basename(file)}')

    # generate queries
    for i in range(updates):
        # create directory for each set of queries
        Path(f'{QUERIES_DIR}/{i}').mkdir(parents=True, exist_ok=True)
        for j in range(1, 22 + 1):
            # create output file if not exists
            with open(f'{QUERIES_DIR}/{i}/{j}.sql', 'w+') as output_file:
                # call qgen
                subprocess.run([f'{DBGEN_DIR}/qgen',
                                f'{j}',
                                '-p', f'{i}',
                                '-r', f'{START_SEED + i}'],
                               cwd=DBGEN_DIR,
                               env=dict(os.environ, DSS_QUERY=f'{DBGEN_DIR}/queries'),
                               stdout=output_file)