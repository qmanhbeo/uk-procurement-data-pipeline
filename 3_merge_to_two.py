import os
import glob
import pandas as pd
from tqdm import tqdm

def merge_dataset(dataset_name: str) -> None:
    """
    Stream-merge all daily Excel files for a given dataset
    (i.e. 'find_a_tender', 'contracts_finder')
    into a single CSV in the 'merged_data' folder,
    without keeping all data in RAM at once.
    """
    # 1. Script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 2. Input + output paths
    input_dir = os.path.join(script_dir, "extracted_data", dataset_name)
    output_dir = os.path.join(script_dir, "merged_data")
    output_file = os.path.join(output_dir, f"{dataset_name}_merged.csv")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # If output file exists from previous runs, remove it to avoid appending twice
    if os.path.exists(output_file):
        print(f"\nExisting output found for {dataset_name}, removing: {output_file}")
        os.remove(output_file)

    # 3. All matching Excel files
    pattern = os.path.join(input_dir, f"{dataset_name}_????_??_??.xlsx")
    files = sorted(glob.glob(pattern))

    print(f"\n=== Merging dataset: {dataset_name} ===")
    print(f"Looking for files matching: {pattern}")
    print(f"Detected {len(files)} files.")

    if not files:
        print("No input files found. Skipping.")
        return

    # 4. Stream read + append to CSV
    print("Reading files and streaming directly to CSV...")
    first = True  # control header writing

    for f in tqdm(files, desc=f"Merging {dataset_name}", unit="file"):
        try:
            df = pd.read_excel(f)
            df["source_file"] = os.path.basename(f)

            # Append to CSV: write header only on first successful chunk
            df.to_csv(
                output_file,
                mode="w" if first else "a",
                header=first,
                index=False
            )
            first = False

        except Exception as e:
            print(f"\nError reading {f}: {e}")

    if first:
        # If 'first' is still True, no file was successfully written
        print("No readable files were written. Skipping.")
        return

    print(f"Done! Stream-merged {len(files)} files into:\n{output_file}")

def main() -> None:
    # Run for both datasets
    for name in ["contracts_finder", "find_a_tender"]:
        merge_dataset(name)

if __name__ == "__main__":
    main()
