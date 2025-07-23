import xarray as xr
import numpy as np
import pandas as pd
import fsspec


def load_mrms(time_range):
    start, end = time_range
    path = f"s3://noaa-mrms-pds/PrecipRate/00.00/{start:%Y/%m/%d}/*"
    fs = fsspec.filesystem("s3", anon=True)
    files = fs.glob(path)
    datasets = [xr.open_dataset(fs.open(f), engine="h5netcdf") for f in files]
    return xr.concat(datasets, dim="time")

def load_glm(time_range):
    start, end = time_range
    path = f"s3://noaa-goes16/GLM-L2-LCFA/{start:%Y/%j}/*"
    fs = fsspec.filesystem("s3", anon=True)
    files = fs.glob(path)
    datasets = [xr.open_dataset(fs.open(f), engine="netcdf4") for f in files]
    return xr.concat(datasets, dim="time")

def create_feature_grid(mrms_data, glm_data):
    # Example feature: max reflectivity in column
    max_refl = mrms_data["PrecipRate"].max(dim="time")
    
    # Example label: any lightning in GLM data during period
    has_flash = glm_data["event_lat"]
    label = int(len(has_flash) > 0)

    feature_array = max_refl.values.flatten()
    label_array = np.array([label] * len(feature_array))
    
    return feature_array, label_array