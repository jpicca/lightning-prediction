from lightning_features.utils import load_mrms, load_glm, create_feature_grid
import xarray as xr

def extract_features(time_range, output_path):
    mrms_data = load_mrms(time_range)
    glm_data = load_glm(time_range)

    features, labels = create_feature_grid(mrms_data, glm_data)

    ds = xr.Dataset({
        "features": ("sample", features),
        "labels": ("sample", labels)
    })
    ds.to_zarr(output_path, mode="w")