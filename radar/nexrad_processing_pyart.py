import glob
import sys

import joblib
import pandas as pd

import pyart_utils as pu




def _dump_interp_res(interp_res, path):
    interped, (lon, lat), xx, yy = interp_res
    df = pd.DataFrame({"dbzh": interped.reshape(-1),
                       "lon": lon.reshape(-1),
                       "lat": lat.reshape(-1),
                       "xx": xx.reshape(-1), 
                       "yy": yy.reshape(-1)})
    df.to_csv(path + ".csv", encoding='utf-8')

def process(f):
    print("processing {}".format(f))
    try:
        res, radar = pu.load_filter_dbzh(f, False, False, False, True)
        interp1 = pu.interp_radar_values(res, pu.MN_INIT_CUT, pu.LOWER_MN)
        _dump_interp_res(interp1, f + ".raw")
        pu._filter_rhov(res.dbzh, radar.get_field(0, 'cross_correlation_ratio'))
        pu._filter_dbzh(res.dbzh)
        pu._filter_v(res.dbzh, radar.get_field(0, 'velocity'))
        interp2 = pu.interp_radar_values(res, pu.MN_INIT_CUT, pu.LOWER_MN)
        _dump_interp_res(interp2, f + ".filtered")
        return None
    except (OSError, ValueError, IndexError) as e:
        return f + "---" + str(e)
    

def main(year, month, day):
    directory = "data/{}-{}/{}".format(year, month, day)
    files = glob.glob("{}/*.ar2v".format(directory))
    p = joblib.Parallel(n_jobs=64, backend='multiprocessing', verbose=1)
    futs = [joblib.delayed(process)(f) for f in files if 'MDM' not in f]
    results = p(futs)
    with open(directory + "/failures.txt", "w+") as out_f:
        out_f.writelines("\n".join(str(x) for x in set(results)))

    
    


if __name__ == "__main__":
    year, month, day = sys.argv[1:4]
    main(year, month, day)
