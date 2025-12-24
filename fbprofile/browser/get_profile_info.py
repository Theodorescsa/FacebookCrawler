import time
import json
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# Import logger từ hệ thống log hiện tại
from logs.loging_config import logger

# ==========================================
# 1. BASIC INFO (Tên, Avatar, Follower)
# ==========================================
def get_name_followers_following_avatar(driver):
    """
    Lấy thông tin cơ bản: Tên, Followers, Following, Avatar, Cover và SỐ LƯỢNG BẠN BÈ.
    """
    info = {
        "name": None,
        "followers": "0",
        "following": "0",
        "friends": "0",      # Thêm trường này
        "avatar_url": None,
        "cover_photo": None
    }
    
    try:
        wait = WebDriverWait(driver, 10)
        
        # 1. Tên (Giữ nguyên)
        try:
            name_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            info["name"] = name_element.text.strip()
        except:
            logger.warning("[PROFILE] Không tìm thấy tên user.")

        # 2. Avatar (CẬP NHẬT MỚI DỰA TRÊN HTML BẠN GỬI)
        try:
            # Tìm thẻ <image> nằm trong <svg role="img">
            # Thuộc tính preserveAspectRatio="xMidYMid slice" rất đặc trưng cho avatar FB
            avatar_xpath = "//*[name()='svg'][@role='img']//*[name()='image']"
            
            # Lấy tất cả các element khớp (thường avatar là cái to nhất hoặc đầu tiên)
            imgs = driver.find_elements(By.XPATH, avatar_xpath)
            
            for img in imgs:
                # Ưu tiên lấy xlink:href
                src = img.get_attribute("xlink:href")
                if not src:
                    src = img.get_attribute("href")
                
                # Link avatar thường chứa 'fbcdn' và không phải là icon nhỏ (thường icon nhỏ là .png hoặc svg base64)
                if src and "fbcdn" in src:
                    info["avatar_url"] = src
                    break # Lấy được cái đầu tiên hợp lệ thì dừng
        except Exception as e:
            logger.warning(f"[PROFILE] Lỗi lấy Avatar: {e}")

        # 3. Số lượng Bạn bè (CẬP NHẬT MỚI)
        try:
            # Tìm thẻ <a> có href chứa chữ 'friends'
            # HTML: <a href=".../friends/"><strong>324</strong> người bạn</a>
            friend_xpath = "//a[contains(@href, 'friends')]//strong"
            friend_element = driver.find_element(By.XPATH, friend_xpath)
            info["friends"] = friend_element.text.strip()
        except:
            # Fallback: Đôi khi nó hiện "xxx người theo dõi" ở chỗ bạn bè nếu không công khai bạn bè
            pass

        # 4. Followers (Người theo dõi - Giữ nguyên logic cũ nhưng thêm try-except lỏng hơn)
        try:
            followers_element = driver.find_element(By.XPATH, "//a[contains(@href, 'followers')]//strong")
            info["followers"] = followers_element.text.strip()
        except:
            pass

        # 5. Following (Đang theo dõi - Giữ nguyên)
        try:
            following_element = driver.find_element(By.XPATH, "//a[contains(@href, 'following')]//strong")
            info["following"] = following_element.text.strip()
        except:
            pass

        # 6. Ảnh bìa (Giữ nguyên)
        try:
            cover_element = driver.find_element(By.XPATH, "//img[@data-imgperflogname='profileCoverPhoto']")
            info["cover_photo"] = cover_element.get_attribute("src")
        except:
            pass

    except Exception as e:
        logger.error(f"[PROFILE] Lỗi lấy Basic Info: {e}")
        
    return info

# ==========================================
# 2. FEATURED NEWS (Tin nổi bật / Highlights)
# ==========================================
def get_profile_featured_news(driver, target_url, timeout: int = 20):
    """Lấy dữ liệu từ mục 'Đáng chú ý' (Highlights)."""
    featured_data = []
    wait = WebDriverWait(driver, timeout)

    try:
        if target_url not in driver.current_url:
            driver.get(target_url)
            time.sleep(3)

        logger.info("[PROFILE] Đang tìm các bộ sưu tập đáng chú ý...")
        
        collection_links = []
        try:
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
                
                if url and url not in [x['url'] for x in collection_links]:
                    collection_links.append({"url": url, "title": title})
        except TimeoutException:
            logger.info("[PROFILE] Không tìm thấy mục Đáng chú ý nào.")
            return []

        logger.info(f"[PROFILE] --> Tìm thấy {len(collection_links)} bộ sưu tập.")

        for collection in collection_links:
            logger.info(f"[PROFILE] Đang quét Highlight: {collection['title']}")
            driver.get(collection['url'])
            time.sleep(4)

            # Xử lý nút "Nhấp để xem tin"
            try:
                view_btn_xpath = "//span[contains(text(), 'Nhấp để xem tin')]"
                overlay_wait = WebDriverWait(driver, 5)
                btn = overlay_wait.until(EC.element_to_be_clickable((By.XPATH, view_btn_xpath)))
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3)
            except TimeoutException:
                pass
            except Exception as e:
                logger.warning(f"[PROFILE] ! Cảnh báo nút xem tin: {e}")

            collection_media = []
            visited_urls = set()

            while True:
                try:
                    media_src = None
                    media_type = "unknown"

                    try:
                        video_element = driver.find_element(By.TAG_NAME, "video")
                        media_src = video_element.get_attribute("src")
                        media_type = "video"
                    except:
                        try:
                            img_element = driver.find_element(By.XPATH, "//div[contains(@data-id, 'story-viewer')]//img")
                            media_src = img_element.get_attribute("src")
                            media_type = "image"
                        except:
                            pass

                    if media_src and media_src not in visited_urls:
                        visited_urls.add(media_src)
                        collection_media.append({"type": media_type, "src": media_src})

                    # Click Next
                    next_xpath = "//div[@aria-label='Thẻ tiếp theo'][@role='button']"
                    try:
                        next_btn = driver.find_element(By.XPATH, next_xpath)
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(2.5)
                    except:
                        break # Hết story
                
                except Exception:
                    break
            
            featured_data.append({
                "collection_title": collection['title'],
                "collection_url": collection['url'],
                "media_items": collection_media
            })

    except Exception as e:
        logger.error(f"[PROFILE] Lỗi Featured News: {str(e)}")

    return featured_data

# ==========================================
# 3. INTRODUCES (Giới thiệu / About)
# ==========================================
def get_profile_introduces(driver, target_url, timeout: int = 15) -> dict:
    """Lấy thông tin Giới thiệu (About)."""
    current_url = driver.current_url
    target_about = f"{target_url}/about" if "profile.php" not in target_url else f"{target_url}&sk=about"
    
    if target_about not in current_url:
        driver.get(target_about)
        time.sleep(3)
    
    data = {}
    wait = WebDriverWait(driver, timeout)

    tabs_mapping = {
        "overview": ["Tổng quan", "Overview"],
        "work_education": ["Công việc và học vấn", "Work and education"],
        "places": ["Nơi từng sống", "Places Lived"],
        "contact_basic": ["Thông tin liên hệ và cơ bản", "Contact and basic info"],
        "family": ["Gia đình và các mối quan hệ", "Family and relationships"],
        "details": ["Chi tiết về", "Details about"],
        "life_events": ["Sự kiện trong đời", "Life events"]
    }

    logger.info("[PROFILE] Đang quét thông tin Giới thiệu...")

    for key, keywords in tabs_mapping.items():
        data[key] = []
        try:
            xpath_parts = [f"contains(text(), '{kw}')" for kw in keywords]
            xpath_condition = " or ".join(xpath_parts)
            xpath_tab = f"//a[@role='tab']//span[{xpath_condition}]"
            
            try:
                tab_element = wait.until(EC.presence_of_element_located((By.XPATH, xpath_tab)))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab_element)
                driver.execute_script("arguments[0].click();", tab_element)
                time.sleep(2)
            except TimeoutException:
                continue

            # Xử lý riêng cho tab "details"
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
                if not rows:
                    rows = driver.find_elements(By.XPATH, "//div[@class='x1iyjqo2']//div[@class='x1gslohp']")
                
                for row in rows:
                    text_content = row.text.strip()
                    if text_content and "Không có" not in text_content and "để hiển thị" not in text_content:
                        clean_text = text_content.replace("\n", " ")
                        if clean_text not in data[key]:
                            data[key].append(clean_text)

        except Exception:
            continue

    return data

# ==========================================
# 4. PHOTOS (Ảnh)
# ==========================================
def get_profile_pictures(driver, target_url, timeout: int = 20) -> list:
    """Lấy danh sách Ảnh."""
    image_urls = []
    wait = WebDriverWait(driver, timeout)

    try:
        target_photos = f"{target_url}/photos" if "profile.php" not in target_url else f"{target_url}&sk=photos"
        driver.get(target_photos)
        time.sleep(3)
        
        logger.info("[PROFILE] Đang quét danh sách ảnh...")
        xpath_images = "//a[contains(@href, 'photo.php')]//img"
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, xpath_images)))
            # Cuộn một chút để load thêm ảnh
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            
            img_elements = driver.find_elements(By.XPATH, xpath_images)
            for img in img_elements:
                src = img.get_attribute("src")
                if src and "fbcdn.net" in src:
                    image_urls.append(src)
        except:
            logger.info("[PROFILE] Không tìm thấy ảnh nào.")
                
    except Exception as e:
        logger.error(f"[PROFILE] Lỗi lấy ảnh: {str(e)}")

    return list(set(image_urls))

# ==========================================
# 5. FRIENDS (Bạn bè)
# ==========================================
def get_profile_friends(driver, target_url, timeout: int = 20) -> list:
    """Lấy danh sách Bạn bè (có cuộn trang)."""
    friends_list = []
    
    try:
        target_friends = f"{target_url}/friends" if "profile.php" not in target_url else f"{target_url}&sk=friends"
            
        logger.info(f"[PROFILE] Đang truy cập danh sách bạn bè: {target_friends}")
        driver.get(target_friends)
        time.sleep(3)

        logger.info("[PROFILE] Đang cuộn trang danh sách bạn bè (Max 3 lần scroll)...")
        # Giới hạn scroll để tránh treo tool quá lâu
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(3): 
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        logger.info("[PROFILE] Đang trích xuất dữ liệu bạn bè...")
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
        logger.error(f"[PROFILE] Lỗi lấy bạn bè: {str(e)}")

    return friends_list

# ==========================================
# MAIN ORCHESTRATOR
# ==========================================
def scrape_full_profile_info(driver, target_url: str, output_path: Path):
    """
    Hàm chính điều phối việc lấy TOÀN BỘ thông tin profile và lưu file.
    """
    logger.info(f"--- BẮT ĐẦU QUÉT INFO PROFILE (FULL): {target_url} ---")
    
    full_data = {
        "url": target_url,
        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "basic_info": {},
        "featured_news": [],
        "introduction": {},
        "photos": [],
        "friends": []
    }

    try:
        # 1. Basic Info (Trang chủ)
        if target_url not in driver.current_url:
            driver.get(target_url)
            time.sleep(3)
        full_data["basic_info"] = get_name_followers_following_avatar(driver)
        logger.info("[PROFILE] ✅ Xong Basic Info")

        # 2. Featured News (Highlights) - Chạy luôn
        # Lưu ý: Hàm này tốn thời gian vì phải click xem từng story
        full_data["featured_news"] = get_profile_featured_news(driver, target_url)
        logger.info(f"[PROFILE] ✅ Xong Highlights ({len(full_data['featured_news'])} bộ)")

        # 3. Introduction (About)
        full_data["introduction"] = get_profile_introduces(driver, target_url)
        logger.info("[PROFILE] ✅ Xong Introduction")

        # 4. Photos
        full_data["photos"] = get_profile_pictures(driver, target_url)
        logger.info(f"[PROFILE] ✅ Xong Photos ({len(full_data['photos'])} ảnh)")

        # 5. Friends
        full_data["friends"] = get_profile_friends(driver, target_url)
        logger.info(f"[PROFILE] ✅ Xong Friends ({len(full_data['friends'])} người)")

    except Exception as e:
        logger.error(f"[PROFILE] ❌ Lỗi nghiêm trọng khi quét profile: {e}")
    finally:
        # Quan trọng: Dù thành công hay thất bại, lưu file lại
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(full_data, f, ensure_ascii=False, indent=4)
            logger.info(f"[PROFILE] 💾 Đã lưu FULL info vào: {output_path}")
        except Exception as save_err:
            logger.error(f"[PROFILE] Không thể lưu file: {save_err}")