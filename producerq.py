from consumerq import process_photos, process_videos

PRIORITY_CUSTOMERS = [1, 7, 9,21,34,26,19,49,44,27]


def apply_task(file_path: str, customer_id: str):
    task = None
    if file_path.endswith(("jpg", "jpeg", "png")):
        task = process_photos.s(file_path, customer_id)
    elif file_path.endswith(("mp4", "mkv", "avi","mov")):
        task = process_videos.s(file_path, customer_id)
    if task:
        if customer_id in PRIORITY_CUSTOMERS:
            if file_path.endswith(("jpg","jpeg","png")):
                task.apply_async(queue ="photo_queue",routing_key = "photo", customer_id=customer_id,priority = 9 )  
            elif file_path.endswith(("mp4","mov","avi")):
                task.apply_async(queue ="video_queue",routing_key = "video", customer_id=customer_id,priority = 9 )     
    else:
            if file_path.endswith(("jpg","jpeg","png")):
                task.apply_async(queue ="photo_queue",routing_key = "photo", customer_id=customer_id,priority = 2)  
            elif file_path.endswith(("mp4","mov","avi")):
                task.apply_async(queue ="video_queue",routing_key = "video", customer_id=customer_id,priority = 2 )   

  
if __name__ == "__main__":

    for i in range (1,50):
        apply_task("photo"+str(i)+".jpg",i)
        apply_task("video"+str(i)+".mov",i)