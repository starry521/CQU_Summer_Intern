import time

def progress_bar(progress,total):
    bar_length = 50
    filled_length = int(bar_length * progress // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    percentage = progress / total * 100
    print(f'Progress: [{bar}] {percentage:.2f}% ({progress}/{total})', end='\r')