import rasterio
import rasterio.windows
import rasterio.features
import geopandas as gpd
import numpy as np
from pathlib import Path
import csv, re
import subprocess, sys, time, os

rasterA_folder = Path(r"D:\copernicus")
shapefile_folder = Path(r"F:\areas")
calc_txt = Path(r"F:\calc1.txt")
output_folder = Path(r"F:\1")
output_folder.mkdir(parents=True, exist_ok=True)

def parse_number(s):
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if m:
        return float(m.group())
    return None

values_dict = {}
with open(calc_txt, "r") as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) < 2:
            continue
        key = row[0].strip().lower()
        numbers = [parse_number(x) for x in row[1:] if parse_number(x) is not None]
        if numbers:
            values_dict[key] = numbers

print("\n📘 Loaded value lists:")
for k, v in values_dict.items():
    print(f"  • {k}: {v}")

rasterA_files = list(rasterA_folder.glob("*.tif"))
if not rasterA_files:
    raise FileNotFoundError("No .tif!")
rasterA_path = rasterA_files[0]
print(f"\n📁 Using rasterA: {rasterA_path.name}")

shapefiles = list(shapefile_folder.glob("*.shp"))
if not shapefiles:
    raise FileNotFoundError("No shapefiles")

print("\n📁 Found shapefiles:")
for s in shapefiles:
    print("  •", s.stem)

processed = []

def crop_and_filter_raster_chunked(src, gdf, target_values, dst_path, chunk_size=512):
    """
    Crop raster to the shapefile,
    chunks to save memory.
    """
    gdf = gdf[gdf.geometry.notnull()]
    gdf["geometry"] = gdf["geometry"].buffer(0)
    shapes = [geom for geom in gdf.geometry]

    xmin, ymin, xmax, ymax = gdf.total_bounds
    window = rasterio.windows.from_bounds(xmin, ymin, xmax, ymax, src.transform)
    window = rasterio.windows.Window(
        col_off=int(window.col_off),
        row_off=int(window.row_off),
        width=int(window.width),
        height=int(window.height)
    )

    out_meta = src.meta.copy()
    out_meta.update({
        "height": int(window.height),
        "width": int(window.width),
        "transform": src.window_transform(window),
        "dtype": "float32",
        "count": 1,
        "nodata": 0
    })

    with rasterio.open(dst_path, "w", **out_meta) as dst:
        for r_off in range(0, int(window.height), chunk_size):
            for c_off in range(0, int(window.width), chunk_size):
                rh = min(chunk_size, int(window.height) - r_off)
                cw = min(chunk_size, int(window.width) - c_off)

                sub_window = rasterio.windows.Window(
                    col_off=int(window.col_off + c_off),
                    row_off=int(window.row_off + r_off),
                    width=int(cw),
                    height=int(rh)
                )
                chunk = src.read(1, window=sub_window)

                transform_chunk = src.window_transform(sub_window)
                poly_mask = rasterio.features.rasterize(
                    [(geom, 1) for geom in shapes],
                    out_shape=(rh, cw),
                    transform=transform_chunk,
                    fill=0,
                    all_touched=True,
                    dtype=np.uint8
                )

                mask_chunk = np.zeros_like(chunk, dtype=np.float32)
                for val in target_values:
                    mask_chunk[(chunk == val) & (poly_mask == 1)] = 1.0

                dst.write(mask_chunk, 1, window=rasterio.windows.Window(c_off, r_off, cw, rh))

for shp in shapefiles:
    shp_name = shp.stem.lower()
    print(f"\n Processing shapefile: {shp.stem}")

    if shp_name not in values_dict:
        print(f"No match calc1.txt {shp.stem}")
        continue

    target_values = values_dict[shp_name]
    print(f"Matched calc1.txt: {shp_name} → values: {target_values}")

    output_path = output_folder / f"{shp.stem}.tif"

    try:
        with rasterio.open(rasterA_path) as srcA:
            gdf = gpd.read_file(shp)

            if gdf.crs != srcA.crs:
                gdf = gdf.to_crs(srcA.crs)

            crop_and_filter_raster_chunked(srcA, gdf, target_values, output_path, chunk_size=512)

            print(f"Saved: {output_path}")
            processed.append(shp.stem)

    except Exception as e:
        print(f"Error {shp.stem}: {e}")
        continue

print("\n============== SO... ==============")
if processed:
    print("Processed shapefiles:")
    for name in processed:
        print("   •", name)
else:
    print("No shapefiles processed.")
print("=====================================\n")

wait_time = 15
print(f"\n Proceed with altitudes? You have {wait_time} seconds to cancel (Ctrl+C).")
for i in range(wait_time, 0, -1):
    print(f"Proceeding in {i}'' ", end='\r', flush=True)
    time.sleep(1)

file2_path = os.path.join(os.path.dirname(__file__), r"F:\altitudes.py")
subprocess.run([sys.executable, file2_path])
print("Altitudes executed!")
