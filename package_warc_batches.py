# package_batches.py
import os
import tarfile
import shutil
from tqdm import tqdm

# ========== 配置 (应与下载脚本保持一致) ==========
# 存放所有批次文件夹的根目录
OUTPUT_DIR = "guardian_world_warc_segments"
# 每个批次文件夹中的最大文件数
MAX_FILES_PER_DIR = 5000


def create_tarball(source_dir: str):
    """将指定的源目录打包成tar.gz文件，然后删除原目录"""
    dir_name = os.path.basename(source_dir)
    archive_name = f"{dir_name}.tar.gz"
    archive_path = os.path.join(os.path.dirname(source_dir), archive_name)

    print(f"\n正在打包 '{source_dir}' -> '{archive_path}'...")
    
    try:
        files_to_add = os.listdir(source_dir)
        with tarfile.open(archive_path, "w:gz") as tar:
            for file_name in tqdm(files_to_add, desc=f"打包 {dir_name}"):
                file_path = os.path.join(source_dir, file_name)
                tar.add(file_path, arcname=file_name) # arcname避免在压缩包内创建完整路径

        print(f"✅ 打包成功: '{archive_path}'")
        
        # 打包成功后，删除原始目录
        print(f"正在删除原始目录: '{source_dir}'...")
        shutil.rmtree(source_dir)
        print(f"✅ 已删除 '{source_dir}'")

    except Exception as e:
        print(f"❌ 打包或删除 '{source_dir}' 时发生错误: {e}")
        # 如果出错，可以选择删除可能已创建的不完整压缩包
        if os.path.exists(archive_path):
            os.remove(archive_path)
            print(f"已删除不完整的压缩包: '{archive_path}'")


def main():
    print("======= 批次打包脚本 =======")
    if not os.path.isdir(OUTPUT_DIR):
        print(f"错误: 根目录 '{OUTPUT_DIR}' 不存在。")
        return

    # 1. 查找所有批次目录
    try:
        all_dirs = [d for d in os.listdir(OUTPUT_DIR) if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and d.startswith("batch_")]
    except FileNotFoundError:
        all_dirs = []

    if not all_dirs:
        print("未找到任何批次目录。")
        return

    # 2. 识别当前活动的目录，我们不应该打包它
    sorted_dirs = sorted(all_dirs)
    active_dir_name = sorted_dirs[-1]
    print(f"当前活动的下载目录是 '{active_dir_name}' (将被跳过)。")

    # 3. 找出所有需要打包的目录
    # 一个目录需要被打包，如果：
    # a) 它不是当前活动的目录
    # b) 它的文件数量达到了阈值
    dirs_to_archive = []
    for dir_name in sorted_dirs:
        if dir_name == active_dir_name:
            continue
        
        dir_path = os.path.join(OUTPUT_DIR, dir_name)
        try:
            num_files = len(os.listdir(dir_path))
            if num_files >= MAX_FILES_PER_DIR:
                dirs_to_archive.append(dir_path)
            else:
                print(f"跳过 '{dir_name}' (文件数 {num_files} < {MAX_FILES_PER_DIR})")
        except FileNotFoundError:
            continue

    if not dirs_to_archive:
        print("\n没有找到需要打包的已满批次目录。")
    else:
        print(f"\n找到 {len(dirs_to_archive)} 个可以打包的目录。")
        for dir_path in dirs_to_archive:
            create_tarball(dir_path)
            
    print("\n======= 打包完成 =======")


if __name__ == "__main__":
    main()