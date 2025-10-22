# package_batches.py
import os
import tarfile
import shutil
import tarfile  # 确保 tarfile 被导入，以便在类型提示中使用
from typing import Optional # 用于类型提示
from tqdm import tqdm

# ========== 配置 (应与下载脚本保持一致) ==========
# 存放所有批次文件夹的根目录
OUTPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments"

def verify_tarball(archive_path: str) -> bool:
    """
    校验tar.gz压缩包的完整性。
    它会尝试读取压缩包中的所有文件头信息。
    如果成功完成且没有异常，则认为压缩包是完整的。
    """
    print(f"正在校验 '{os.path.basename(archive_path)}'...")
    try:
        with tarfile.open(archive_path, "r:gz") as tar:
            for member in tqdm(tar.getmembers(), desc="校验中", unit=" files"):
                pass
        print(f"✅ 校验成功: '{archive_path}' 结构完整。")
        return True
    except tarfile.ReadError as e:
        print(f"❌ 校验失败: 压缩包已损坏或不完整。错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 校验时发生未知错误: {e}")
        return False

# ================== 新增的过滤器函数 ==================
def exclude_filter(tarinfo: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
    """
    用于 tarfile.add() 的过滤器。
    如果文件名以 '._' 开头，则排除该文件。
    这些是 macOS 在 ExFAT/FAT32 驱动器上创建的元数据文件。
    """
    file_name = os.path.basename(tarinfo.name)
    if file_name.startswith("._"):
        # 返回 None 会告诉 tarfile 跳过这个文件
        return None
    else:
        # 返回 tarinfo 对象以正常包含该文件
        return tarinfo
# ======================================================

def create_tarball(source_dir: str):
    """将指定的源目录打包成tar.gz文件，然后校验其完整性，并排除 ._ 文件。"""
    dir_name = os.path.basename(source_dir)
    archive_name = f"{dir_name}.tar.gz"
    archive_path = os.path.join(os.path.dirname(source_dir), archive_name)

    if os.path.exists(archive_path):
        print(f"\n⚠️ 跳过 '{dir_name}'，因为压缩包 '{archive_name}' 已存在。")
        return

    print(f"\n正在打包 '{source_dir}' -> '{archive_path}'...")
    
    try:
        print(f"正在添加 '{dir_name}' 的内容到压缩包 (将排除 '._' 文件)...")
        with tarfile.open(archive_path, "w:gz") as tar:
            # ============ 在这里使用 filter 参数 ============
            tar.add(source_dir, arcname='.', filter=exclude_filter)
            # =============================================

        print(f"✅ 打包成功: '{archive_path}'")
        
        is_valid = verify_tarball(archive_path)

        if not is_valid:
            print(f"正在删除已损坏的压缩包: '{archive_path}'")
            os.remove(archive_path)

    except Exception as e:
        print(f"❌ 打包 '{source_dir}' 时发生严重错误: {e}")
        if os.path.exists(archive_path):
            os.remove(archive_path)
            print(f"已删除不完整的压缩包: '{archive_path}'")


def main():
    print("======= 批次打包脚本 (仅打包与校验, 排除 ._ 文件) =======")
    if not os.path.isdir(OUTPUT_DIR):
        print(f"错误: 根目录 '{OUTPUT_DIR}' 不存在。")
        return

    try:
        all_dirs = [d for d in os.listdir(OUTPUT_DIR) if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and d.startswith("batch_")]
    except FileNotFoundError:
        all_dirs = []

    if not all_dirs:
        print("未找到任何批次目录。")
        return

    sorted_dirs = sorted(all_dirs)
    
    if len(sorted_dirs) > 1:
        dirs_to_archive = sorted_dirs[:-1]
    else:
        dirs_to_archive = []

    if not dirs_to_archive:
        print("\n没有找到需要打包的已完成批次目录。")
        active_dir_name = sorted_dirs[-1] if sorted_dirs else "无"
        print(f"当前活动的目录是 '{active_dir_name}'。")
    else:
        print(f"\n找到 {len(dirs_to_archive)} 个可以打包的目录。")
        for dir_name in dirs_to_archive:
            dir_path = os.path.join(OUTPUT_DIR, dir_name)
            create_tarball(dir_path)
            
    print("\n======= 打包与校验完成 =======")
    print("请手动检查并删除原始的 batch_XXXX 文件夹。")


if __name__ == "__main__":
    main()