import os
import bz2
import gzip
import zipfile
import tarfile
import shutil
from pathlib import Path


def unpack_file(input_path, output_dir):
    """Unpack a single compressed file to the output directory."""
    filename = os.path.basename(input_path)
    
    try:
        # Handle .bz2 files
        if filename.endswith('.bz2'):
            output_filename = filename[:-4]  # Remove .bz2 extension
            output_path = os.path.join(output_dir, output_filename)
            
            with bz2.open(input_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Unpacked: {filename} -> {output_filename}")
            return True
        
        # Handle .gz files
        elif filename.endswith('.gz') and not filename.endswith('.tar.gz'):
            output_filename = filename[:-3]  # Remove .gz extension
            output_path = os.path.join(output_dir, output_filename)
            
            with gzip.open(input_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Unpacked: {filename} -> {output_filename}")
            return True
        
        # Handle .zip files
        elif filename.endswith('.zip'):
            with zipfile.ZipFile(input_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            print(f"Unpacked: {filename}")
            return True
        
        # Handle .tar files (including .tar.gz, .tar.bz2)
        elif filename.endswith(('.tar', '.tar.gz', '.tar.bz2')):
            with tarfile.open(input_path, 'r:*') as tar_ref:
                tar_ref.extractall(output_dir)
            print(f"Unpacked: {filename}")
            return True
        
        else:
            # Copy non-compressed files as-is
            output_path = os.path.join(output_dir, filename)
            shutil.copy2(input_path, output_path)
            print(f"Copied: {filename}")
            return True
            
    except Exception as e:
        print(f"Error unpacking {filename}: {e}")
        return False


def unpack_directory(input_dir, output_dir):
    """Unpack all files from input directory to output directory."""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all files in input directory
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"Error: Input directory '{input_dir}' does not exist!")
        return
    
    # Get all files (recursively)
    files = list(input_path.rglob('*'))
    files = [f for f in files if f.is_file()]
    
    if not files:
        print(f"No files found in '{input_dir}'")
        return
    
    print(f"Found {len(files)} files to process")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print("-" * 60)
    
    success_count = 0
    error_count = 0
    
    for file_path in files:
        # Preserve directory structure in output
        relative_path = file_path.relative_to(input_path)
        relative_dir = relative_path.parent
        
        # Create subdirectory in output if needed
        output_subdir = os.path.join(output_dir, relative_dir)
        os.makedirs(output_subdir, exist_ok=True)
        
        # Unpack the file
        if unpack_file(str(file_path), output_subdir):
            success_count += 1
        else:
            error_count += 1
    
    print("-" * 60)
    print(f"Completed: {success_count} files processed successfully")
    if error_count > 0:
        print(f"Errors: {error_count} files failed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Unpack compressed files from input directory to output directory')
    parser.add_argument('--input', default='football_data_zip', 
                        help='Input directory containing files to unpack (default: football_data_zip)')
    parser.add_argument('--output', default='football_data', 
                        help='Output directory for unpacked files (default: football_data)')
    
    args = parser.parse_args()
    
    unpack_directory(args.input, args.output)

