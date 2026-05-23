# naver_crawler_engine

Rust 기반 크롤러 모음입니다. 대부분 ChromeDriver를 먼저 켠 뒤, 다른 터미널에서 `cargo run --bin naver_crawler_engine -- <subcommand> [options]` 형태로 실행합니다.

## 1. 공통 준비

작업 폴더로 이동합니다.

```powershell
cd C:\Users\choho\crawler-rs\crawler-rs
```

ChromeDriver를 실행합니다.

```powershell
.\chromedriver.exe --port=4444
```

Firefox/GeckoDriver로 `cafe-open`을 실행하려면 GeckoDriver를 대신 켭니다.

```powershell
.\geckodriver.exe --port 4444
```

Firefox를 병렬로 쓰려면 GeckoDriver를 포트별로 여러 개 켭니다. 포트 4444~4451이면 8개 워커용입니다.

```powershell
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4444"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4445"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4446"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4447"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4448"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4449"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4450"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4451"
```

다른 터미널에서 크롤러를 실행합니다.

```powershell
cargo run --bin naver_crawler_engine -- <subcommand> [options]
```

ChromeDriver 포트가 이미 사용 중이면 실행 중인 프로세스를 확인합니다.

```powershell
Get-Process chromedriver -ErrorAction SilentlyContinue
```

필요하면 종료 후 다시 켭니다.

```powershell
Stop-Process -Name chromedriver -Force
.\chromedriver.exe --port=4444
```

다른 포트를 쓰고 싶으면 ChromeDriver 포트와 `--webdriver` 값을 같이 바꿉니다.

```powershell
.\chromedriver.exe --port=9515
```

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-posts 100 --webdriver http://localhost:9515 --out-dir out
```

빌드 확인 명령어입니다.

```powershell
cargo check --workspace
cargo test plan_f --workspace
```

## 2. 네이버 스마트스토어 리뷰

URL 파일을 사용하는 기본 명령입니다. `url.txt`는 한 줄에 URL 하나씩 넣습니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --input .\url.txt --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

URL을 직접 여러 개 넣을 수도 있습니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --url "https://smartstore.naver.com/store/products/111" --url "https://smartstore.naver.com/store/products/222" --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

한 줄씩 안정적으로 돌릴 때는 워커를 1로 둡니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --input .\url.txt --workers 1 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

## 3. 네이버 카페 공개/미가입 접근 크롤러

`f-e/cafes/.../menus/...` 주소를 그대로 넣으면 됩니다. 가입/미가입 여부와 상관없이 먼저 목록 URL을 수집하고, 상세 글은 별도 워커로 수집합니다.

URL만 저장하려면 `--url-only`를 붙입니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-posts 500 --list-workers 5 --url-only --webdriver http://localhost:4444 --out-dir out
```

GeckoDriver를 쓸 때는 `--browser firefox`를 붙입니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 10 --list-workers 1 --url-only --browser firefox --webdriver http://localhost:4444 --out-dir out
```

GeckoDriver 여러 포트를 워커에 분산하려면 `--webdriver`를 반복해서 넘깁니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 200 --list-workers 8 --url-only --browser firefox --webdriver http://localhost:4444 --webdriver http://localhost:4445 --webdriver http://localhost:4446 --webdriver http://localhost:4447 --webdriver http://localhost:4448 --webdriver http://localhost:4449 --webdriver http://localhost:4450 --webdriver http://localhost:4451 --out-dir out
```

URL 수집과 상세 수집을 한 번에 실행합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-posts 500 --list-workers 5 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

목록 페이지 수를 직접 지정하려면 `--max-pages`를 씁니다. 아래 명령은 `page=1&size=50` 기준으로 1페이지부터 10페이지까지 URL을 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 10 --list-workers 5 --url-only --webdriver http://localhost:4444 --out-dir out
```

`--list-workers`는 목록 URL 수집용 워커 수입니다. `--workers`는 상세 글 수집용 워커 수입니다.

```text
--list-workers 5  = 목록 1, 2, 3, 4, 5페이지를 여러 Chrome 세션이 나눠 수집
--workers 3       = 수집된 글 URL 상세 페이지를 3개 Chrome 세션이 나눠 수집
```

이전 실행에서 저장된 URL CSV만으로 상세 수집을 다시 실행할 수 있습니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url-csv "out\cafe_open_rows_001-500_20260523_120000_urls.csv" --from-row 1 --to-row 50 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

특정 행만 다시 수집할 때는 `--from-row`, `--to-row`를 씁니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url-csv "out\cafe_open_rows_001-500_20260523_120000_urls.csv" --from-row 51 --to-row 100 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

출력 파일명은 실행 시각 기준으로 자동 생성됩니다.

```text
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_urls.csv
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_results.csv
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_comments.csv
```

`results.csv`의 `comments` 컬럼은 여러 댓글을 ` | `로 합쳐 한 행에 저장합니다. 댓글을 행 단위로 따로 보려면 `comments.csv`를 사용합니다.

## 4. 쿠팡 리뷰

기본 API 수집 명령입니다. 작업 단위는 `상품 URL + 리뷰 페이지 번호`입니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 3 --output out\coupang_reviews_001_010.csv
```

11페이지부터 20페이지까지 이어서 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 11 --max-pages 10 --workers 3 --output out\coupang_reviews_011_020.csv
```

URL 파일을 사용할 수도 있습니다. `coupang_urls.txt`는 한 줄에 상품 URL 하나씩 넣습니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --input .\coupang_urls.txt --start-page 1 --max-pages 10 --workers 3 --output out\coupang_reviews.csv
```

403 Access Denied가 나오면 브라우저에서 복사한 Cookie 헤더를 파일에 저장하고 `--cookie-file`을 붙입니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 1 --cookie-file .\coupang_cookie.txt --page-delay-ms 1500 --output out\coupang_reviews.csv
```

쿠키를 넣어도 403이면 ChromeDriver를 켠 뒤 `--browser-fetch`를 사용합니다.

```powershell
.\chromedriver.exe --port=4444
```

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 1 --cookie-file .\coupang_cookie.txt --browser-fetch --webdriver http://localhost:4444 --page-delay-ms 1500 --output out\coupang_reviews.csv
```

출력 컬럼은 아래와 같습니다.

```text
product_url, product_id, page, idx_in_page, product_title, product_option, author, rating, date, helpful_count, headline, review_body, survey_answer, raw_text
```

## 5. 잇다 커뮤니티

처음 실행하면 로그인용 Chrome 창이 뜹니다. 로그인 후 터미널에서 Enter를 누르면 `/community?page=1`로 돌아가 URL을 수집하고, 각 글의 날짜, 본문, 댓글을 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 2 --workers 10 --webdriver http://localhost:4444 --out-dir out
```

전체 페이지 수를 길게 잡는 예시입니다.

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 43 --max-posts 0 --workers 3 --webdriver http://localhost:4444 --out-dir out --profile-dir target\itda_login_profile
```

출력 파일입니다.

```text
out\itda_community.csv
```

댓글이 여러 개면 `comment_body`에 ` | `로 합쳐 저장합니다.

## 6. 네이버 검색 / 블로그 / 티스토리

테스트로 10개만 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=..." --max-posts 10 --max-scrolls 5 --workers 2 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 20
```

무한 스크롤 결과를 많이 수집하려면 `--max-posts 0`으로 두고 `--max-scrolls`를 크게 잡습니다.

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=..." --max-posts 0 --max-scrolls 80 --workers 3 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 50
```

출력 파일입니다.

```text
out\naver_search_posts.csv
out\naver_search_comments.csv
```

`naver_search_posts.csv`의 `comments` 컬럼은 여러 댓글을 ` | `로 합쳐 저장합니다. 댓글을 별도 행으로 분석하려면 `naver_search_comments.csv`를 사용합니다.

## 7. 자주 쓰는 옵션

`--workers`는 상세 페이지를 동시에 열 Chrome 세션 수입니다. 안정성이 중요하면 `1`, 속도가 중요하면 PC 사양에 맞춰 `3~10` 사이에서 조절합니다.

`--list-workers`는 카페 목록 URL 수집 전용 워커 수입니다. `cafe-open`에서만 사용합니다.

`--browser firefox`는 `cafe-open`을 GeckoDriver/Firefox로 실행할 때 씁니다. 생략하면 기본값은 Chrome입니다. Firefox는 GeckoDriver 포트 하나당 세션 하나라서 병렬 수집 시 `--webdriver`를 여러 번 넘겨야 합니다.

`--max-posts`는 최대 수집 글 수입니다. 카페에서 `page=1&size=50 --max-posts 500`이면 대략 10페이지를 대상으로 URL을 모읍니다.

`--max-pages`는 페이지 수를 직접 지정할 때 씁니다.

`--url-only`는 URL CSV만 저장하고 상세 수집을 건너뜁니다.

`--from-row`, `--to-row`는 저장된 URL CSV 기준으로 특정 행 범위만 상세 수집할 때 씁니다.

`cafe-open`의 페이지 로드 timeout은 180초입니다. URL만 수집할 때 게시글 목록 테이블을 기다리는 시간도 최대 180초입니다. 느린 카페 페이지나 Chrome 렌더러 지연 때문에 30초 전후로 끊기는 문제를 피하려고 길게 잡아두었습니다.

## 8. Git 커밋 / 푸쉬

현재 변경사항을 확인합니다.

```powershell
git status
```

이번 작업 파일을 스테이징합니다.

```powershell
git add README.md src/main.rs src/plan_b.rs src/plan_f.rs src/plan_o.rs
```

커밋합니다.

```powershell
git commit -m "feat: add crawler command workflows"
```

현재 브랜치 그대로 원격에 푸쉬합니다.

```powershell
git push origin HEAD
```

브랜치 이름을 확인하고 명시적으로 푸쉬하려면 아래처럼 실행합니다.

```powershell
git branch --show-current
git push origin 브랜치명
```
