# post/v3/browser/profile_info.py
import sys
import time
import json
import os
from pathlib import Path
from datetime import date

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# --- CẤU HÌNH ĐƯỜNG DẪN IMPORT ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import hàm khởi tạo driver của bạn
from util.export_utils.export_fb_session import start_driver

# ==========================================
# CÁC HÀM XỬ LÝ (HELPER FUNCTIONS)
# ==========================================

def get_name_follwers_following_avatar(driver):
    """
    Lấy thông tin cơ bản: Tên, Followers, Following, Avatar và Ảnh bìa.
    """
    info = {
        "name": None,
        "followers": None,
        "following": None,
        "avatar_url": None,
        "cover_photo": None  # Thêm trường này
    }
    
    try:
        wait = WebDriverWait(driver, 10)
        
        # 1. Tên
        try:
            name_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            info["name"] = name_element.text.strip()
        except:
            print("Không tìm thấy tên.")

        # 2. Followers
        try:
            followers_element = driver.find_element(By.XPATH, "//a[contains(@href, 'followers')]//strong")
            info["followers"] = followers_element.text.strip()
        except:
            info["followers"] = "0"

        # 3. Following
        try:
            following_element = driver.find_element(By.XPATH, "//a[contains(@href, 'following')]//strong")
            info["following"] = following_element.text.strip()
        except:
            info["following"] = "0"

        # 4. Avatar (Ảnh đại diện)
        try:
            # Avatar thường nằm trong thẻ SVG -> image
            avatar_element = driver.find_element(By.XPATH, "//svg[@role='img']//image")
            info["avatar_url"] = avatar_element.get_attribute("xlink:href")
        except:
            pass

        # 5. Ảnh bìa (Cover Photo) - MỚI
        try:
            # Dựa vào thuộc tính data-imgperflogname="profileCoverPhoto" trong HTML bạn gửi
            cover_element = driver.find_element(By.XPATH, "//img[@data-imgperflogname='profileCoverPhoto']")
            info["cover_photo"] = cover_element.get_attribute("src")
        except:
            # Fallback: Đôi khi Facebook load ảnh bìa dạng khác, nhưng đây là cách chuẩn theo HTML bạn đưa
            pass

    except TimeoutException:
        print("Lỗi: Quá thời gian chờ khi lấy thông tin cơ bản.")
    except Exception as e:
        print(f"Lỗi Basic Info: {str(e)}")
        
    return info
def get_profile_featured_news(driver, target_url, timeout: int = 20):
    """
    Hàm lấy dữ liệu từ mục 'Đáng chú ý' (Highlights).
    CẬP NHẬT: Tự động click "Nhấp để xem tin" nếu bị chặn.
    """
    featured_data = []
    wait = WebDriverWait(driver, timeout)

    try:
        # --- BƯỚC 1: VÀO TRANG PROFILE ---
        if target_url not in driver.current_url:
            driver.get(target_url)
            time.sleep(3)

        print("Đang tìm các bộ sưu tập đáng chú ý...")
        
        collection_links = []
        try:
            # Tìm các link highlights
            elements = wait.until(EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(@href, 'source=profile_highlight')]")
            ))
            for el in elements:
                url = el.get_attribute("href")
                title = el.text.strip()
                if not title:
                    try:
                        title = el.find_element(By.XPATH, ".//span[contains(@style, '-webkit-line-clamp')]").text
                    except:
                        title = "Không tên"
                
                # Lọc URL trùng
                if url and url not in [x['url'] for x in collection_links]:
                    collection_links.append({"url": url, "title": title})
        except TimeoutException:
            print("Không tìm thấy mục Đáng chú ý nào.")
            return []

        print(f"--> Tìm thấy {len(collection_links)} bộ sưu tập.")

        # --- BƯỚC 2: DUYỆT QUA TỪNG BỘ SƯU TẬP ---
        for collection in collection_links:
            print(f"    Đang quét: {collection['title']}")
            driver.get(collection['url'])
            time.sleep(4) # Chờ Viewer load ban đầu

            # ============================================================
            # [MỚI] XỬ LÝ NÚT "NHẤP ĐỂ XEM TIN"
            # ============================================================
            try:
                # Tìm thẻ span chứa chữ "Nhấp để xem tin"
                view_btn_xpath = "//span[contains(text(), 'Nhấp để xem tin')]"
                
                # Chờ tối đa 5s xem nút này có hiện không (dùng timeout ngắn để không làm chậm nếu không có)
                overlay_wait = WebDriverWait(driver, 5)
                btn = overlay_wait.until(EC.element_to_be_clickable((By.XPATH, view_btn_xpath)))
                
                print("    -> Phát hiện màn hình chờ, đang click 'Nhấp để xem tin'...")
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3) # Chờ story thật load sau khi click
            except TimeoutException:
                # Nếu không thấy nút này thì tốt, story tự chạy
                pass
            except Exception as e:
                print(f"    ! Cảnh báo nút xem tin: {e}")
            # ============================================================

            collection_media = []
            visited_urls = set()

            while True:
                try:
                    media_src = None
                    media_type = "unknown"

                    # 1. Tìm Video
                    try:
                        video_element = driver.find_element(By.TAG_NAME, "video")
                        media_src = video_element.get_attribute("src")
                        media_type = "video"
                    except:
                        # 2. Nếu không có video, tìm Ảnh
                        try:
                            # XPath ảnh trong viewer
                            img_element = driver.find_element(By.XPATH, "//div[contains(@data-id, 'story-viewer')]//img")
                            media_src = img_element.get_attribute("src")
                            media_type = "image"
                        except:
                            pass

                    # Lưu dữ liệu
                    if media_src and media_src not in visited_urls:
                        # In ra để debug chơi
                        # print(f"      + {media_type}: {media_src[:30]}...")
                        visited_urls.add(media_src)
                        collection_media.append({"type": media_type, "src": media_src})

                    # 3. Click Next (Thẻ tiếp theo)
                    next_xpath = "//div[@aria-label='Thẻ tiếp theo'][@role='button']"
                    try:
                        next_btn = driver.find_element(By.XPATH, next_xpath)
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(2.5) # Chờ story tiếp theo load
                    except:
                        # Không thấy nút Next -> Hết story -> Break vòng lặp while
                        break 
                
                except Exception:
                    break
            
            # Lưu vào list tổng
            featured_data.append({
                "collection_title": collection['title'],
                "collection_url": collection['url'],
                "media_items": collection_media
            })

    except Exception as e:
        print(f"Lỗi Featured News: {str(e)}")

    return featured_data

def get_profile_introduces(driver, target_url, timeout: int = 20) -> dict:
    """
    Lấy thông tin Giới thiệu (About).
    """
    if "profile" not in target_url:
        driver.get(f"{target_url}/about")
    else:
        driver.get(f"{target_url}&sk=about")
    time.sleep(3)
    
    data = {}
    wait = WebDriverWait(driver, timeout)

    tabs_mapping = {
        "overview": ["Tổng quan"],
        "work_education": ["Công việc và học vấn"],
        "places": ["Nơi từng sống"],
        "contact_basic": ["Thông tin liên hệ và cơ bản"],
        "family": ["Gia đình và các mối quan hệ"],
        "details": ["Chi tiết về"],
        "life_events": ["Sự kiện trong đời"]
    }

    print("Đang quét thông tin Giới thiệu...")

    for key, keywords in tabs_mapping.items():
        data[key] = []
        try:
            xpath_tab = f"//a[@role='tab']//span[contains(text(), '{keywords[0]}')]"
            tab_element = wait.until(EC.presence_of_element_located((By.XPATH, xpath_tab)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab_element)
            driver.execute_script("arguments[0].click();", tab_element)
            time.sleep(2) 

            if key == "details":
                sections = driver.find_elements(By.XPATH, "//div[@class='x1iyjqo2']//div[@class='xieb3on x1gslohp']")
                for sec in sections:
                    try:
                        header = sec.find_element(By.TAG_NAME, "h2").text.strip()
                        content_div = sec.find_element(By.XPATH, "./following-sibling::div[contains(@class, 'xat24cr')]")
                        content_text = content_div.text.strip()
                        if "Không có" not in content_text:
                            data[key].append(f"{header}: {content_text}")
                    except:
                        continue
            else:
                rows = driver.find_elements(By.XPATH, "//div[contains(@class, 'x13faqbe')]")
                for row in rows:
                    text_content = row.text.strip()
                    if text_content and "Không có" not in text_content and "để hiển thị" not in text_content:
                        clean_text = text_content.replace("\n", " ")
                        if clean_text not in data[key]:
                            data[key].append(clean_text)

        except TimeoutException:
            pass # Không có tab này
        except Exception:
            continue

    return data

def get_profile_pictures(driver, target_url, timeout: int = 20) -> list:
    """
    Lấy danh sách Ảnh.
    """
    image_urls = []
    wait = WebDriverWait(driver, timeout)

    try:
        if "profile" not in target_url:
            driver.get(f"{target_url}/photos")
        else:
            driver.get(f"{target_url}&sk=photos")
        time.sleep(3)
        
        print("Đang quét danh sách ảnh...")
        xpath_images = "//a[contains(@href, 'photo.php')]//img"
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, xpath_images)))
            img_elements = driver.find_elements(By.XPATH, xpath_images)
            for img in img_elements:
                src = img.get_attribute("src")
                if src and "fbcdn.net" in src:
                    image_urls.append(src)
        except:
            print("Không tìm thấy ảnh nào.")
                
    except Exception as e:
        print(f"Lỗi lấy ảnh: {str(e)}")

    return list(set(image_urls))

def get_profile_friends(driver, target_url, timeout: int = 20) -> list:
    """
    Lấy danh sách Bạn bè (có cuộn trang).
    """
    friends_list = []
    wait = WebDriverWait(driver, timeout)

    try:
        if "profile.php" in target_url:
            friends_url = f"{target_url}&sk=friends"
        else:
            friends_url = f"{target_url}/friends"
            
        print(f"Đang truy cập danh sách bạn bè: {friends_url}")
        driver.get(friends_url)
        time.sleep(3)

        print("Đang cuộn trang (Infinite Scroll)...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        retries = 0
        max_retries = 3
        
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                retries += 1
                if retries >= max_retries:
                    break
            else:
                last_height = new_height
                retries = 0

        print("Đang trích xuất dữ liệu bạn bè...")
        info_divs = driver.find_elements(By.XPATH, "//div[contains(@class, 'x1iyjqo2') and contains(@class, 'xv54qhq')]")

        for info in info_divs:
            try:
                friend_data = {"name": None, "profile_url": None, "avatar_url": None, "subtitle": ""}
                
                # Tên & Link
                try:
                    link_element = info.find_element(By.XPATH, ".//a[@role='link']")
                    friend_data["name"] = link_element.text.strip()
                    friend_data["profile_url"] = link_element.get_attribute("href")
                except: continue

                # Subtitle
                try:
                    sub_el = info.find_element(By.XPATH, ".//div[contains(@class, 'x1gslohp')]")
                    friend_data["subtitle"] = sub_el.text.strip()
                except: pass

                # Avatar
                try:
                    avt_el = info.find_element(By.XPATH, "./preceding-sibling::div//img")
                    friend_data["avatar_url"] = avt_el.get_attribute("src")
                except: pass

                if friend_data["name"]:
                    friends_list.append(friend_data)
            except: continue

    except Exception as e:
        print(f"Lỗi lấy bạn bè: {str(e)}")

    return friends_list

# ==========================================
# HÀM MAIN (CHƯƠNG TRÌNH CHÍNH)
# ==========================================

def main():
    # 1. Cấu hình
    profile_name_driver = "Profile 5" # Tên profile trong tool export_fb_session
    target_url = "https://www.facebook.com/duy.pham.598064"
    
    # Tạo ID file từ URL
    uid = target_url.split("id=")[-1].split("&")[0] if "id=" in target_url else target_url.strip("/").split("/")[-1]
    
    print(f"--- BẮT ĐẦU QUÉT PROFILE: {uid} ---")
    driver = start_driver(profile_name_driver)
    
    # Cấu trúc dữ liệu tổng
    full_data = {
        "id": uid,
        "url": target_url,
        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "basic_info": {},
        "featured_news": [],
        "introduction": {},
        "photos": [],
        "friends": []
    }

    try:
        # BƯỚC 1: Vào trang chủ profile lấy Basic Info
        print("\n[1/5] Lấy thông tin cơ bản...")
        driver.get(target_url)
        time.sleep(4)
        full_data["basic_info"] = get_name_follwers_following_avatar(driver)
        print("✅ Hoàn thành Basic Info.")

        # BƯỚC 2: Lấy Featured News (Tin nổi bật)
        # Lưu ý: Hàm này sẽ tự mở các story viewer
        print("\n[2/5] Lấy tin nổi bật (Highlights)...")
        # full_data["featured_news"] = get_profile_featured_news(driver, target_url)
        # print(f"✅ Hoàn thành Featured News ({len(full_data['featured_news'])} bộ).")

        # BƯỚC 3: Lấy Giới thiệu (About)
        print("\n[3/5] Lấy thông tin Giới thiệu...")
        full_data["introduction"] = get_profile_introduces(driver, target_url)
        print("✅ Hoàn thành Introduction.")

        # BƯỚC 4: Lấy Ảnh (Photos)
        print("\n[4/5] Lấy danh sách Ảnh...")
        full_data["photos"] = get_profile_pictures(driver, target_url)
        print(f"✅ Hoàn thành Photos ({len(full_data['photos'])} ảnh).")

        # BƯỚC 5: Lấy Bạn bè (Friends)
        print("\n[5/5] Lấy danh sách Bạn bè...")
        full_data["friends"] = get_profile_friends(driver, target_url)
        print(f"✅ Hoàn thành Friends ({len(full_data['friends'])} bạn).")

        # --- LƯU FILE ---
        file_name = f"fb_data_{uid}.json"
        print(f"\n💾 Đang lưu kết quả vào file: {file_name}")
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(full_data, f, ensure_ascii=False, indent=4)
        print("🎉 ĐÃ LƯU THÀNH CÔNG!")

    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA TRONG MAIN: {e}")
        import traceback
        traceback.print_exc()
        
        # Cố gắng lưu dữ liệu đã lấy được
        with open(f"fb_data_{uid}_ERROR.json", "w", encoding="utf-8") as f:
            json.dump(full_data, f, ensure_ascii=False, indent=4)
        print("⚠️ Đã lưu file cứu hộ (_ERROR.json)")

    finally:
        print("\n--- Đóng trình duyệt sau 5s ---")
        time.sleep(5)
        driver.quit()

if __name__ == "__main__":
    main()