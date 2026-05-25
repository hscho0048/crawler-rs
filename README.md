# crawler-rs

Rust로 만든 크롤러 모음입니다. 대부분의 기능은 아래 공통 형식으로 실행합니다.

```powershell
cargo run --bin naver_crawler_engine -- <subcommand> [options]
```

Instagram 전용 Plan L만 별도 바이너리입니다.

```powershell
cargo run --bin instagram -- [options]
```

## 0. 공통 준비

작업 폴더로 이동합니다.

```powershell
cd C:\Users\choho\crawler-rs
```

빌드가 되는지 먼저 확인합니다.

```powershell
cargo build
```

전체 명령 목록을 봅니다.

```powershell
cargo run --bin naver_crawler_engine -- --help
```

각 명령의 옵션을 봅니다.

```powershell
cargo run --bin naver_crawler_engine -- kin --help
cargo run --bin naver_crawler_engine -- cafe-open --help
cargo run --bin naver_crawler_engine -- coupang --help
```

ChromeDriver가 필요한 Plan은 먼저 ChromeDriver를 켭니다. 이 repo에는 `chromedriver.exe`가 들어 있습니다.

```powershell
.\chromedriver.exe --port=4444
```

일부 기존 Plan의 기본 WebDriver 포트는 `9515`입니다. README 예시는 헷갈리지 않게 대부분 `--webdriver http://localhost:4444`를 직접 넘깁니다. 만약 9515로 켰다면 명령의 `--webdriver`도 같이 바꾸세요.

```powershell
.\chromedriver.exe --port=9515
```

ChromeDriver가 이미 떠 있는지 확인합니다.

```powershell
Get-Process chromedriver -ErrorAction SilentlyContinue
```

ChromeDriver를 강제로 내리고 다시 켭니다.

```powershell
Stop-Process -Name chromedriver -Force
.\chromedriver.exe --port=4444
```

여러 worker를 진짜 병렬 브라우저로 돌리고 싶으면 포트를 여러 개 열 수 있습니다.

```powershell
Start-Process -FilePath ".\chromedriver.exe" -ArgumentList "--port=4444"
Start-Process -FilePath ".\chromedriver.exe" -ArgumentList "--port=4445"
Start-Process -FilePath ".\chromedriver.exe" -ArgumentList "--port=4446"
```

## 1. Plan A + Plan B fallback: `crawl`

목적: URL 파일이나 `--url`로 받은 네이버 블로그/카페 글을 HTTP 우선으로 수집하고, 필요하면 WebDriver fallback을 붙입니다.

ChromeDriver 필요 여부:

- Plan A만 쓰면 불필요
- `--webdriver`, `--plan-b-pages`를 쓰면 필요

URL 파일 형식:

```text
https://blog.naver.com/example/223000000000
https://cafe.naver.com/example/12345
```

HTTP만으로 빠르게 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- crawl --input .\urls.txt --max-in-flight 100 --out-dir out
```

URL을 직접 여러 개 넘깁니다.

```powershell
cargo run --bin naver_crawler_engine -- crawl --url "https://blog.naver.com/example/223000000000" --url "https://cafe.naver.com/example/12345" --max-in-flight 50 --out-dir out
```

HTTP 실패분을 ChromeDriver로 fallback 처리합니다.

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- crawl --input .\urls.txt --max-in-flight 50 --webdriver http://localhost:4444 --plan-b-pages 3 --out-dir out
```

출력:

```text
out\results.csv
out\comments.csv
```

## 2. Plan B: `cafe`

목적: 로그인 쿠키가 필요한 네이버 카페 게시판을 ChromeDriver로 수집합니다.

ChromeDriver 필요 여부: 필요

쿠키 파일 형식:

```json
[
  { "name": "NID_AUT", "value": "..." },
  { "name": "NID_SES", "value": "..." }
]
```

기본 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- cafe --url "https://cafe.naver.com/cafename/board" --max-posts 100 --workers 3 --webdriver http://localhost:4444 --cookie-file .\cookies.json --out-dir out
```

쿠키 없이 공개 카페를 시험합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe --url "https://cafe.naver.com/cafename/board" --max-posts 20 --workers 1 --webdriver http://localhost:4444 --out-dir out
```

출력:

```text
out\results.csv
out\comments.csv
```

## 3. Plan C: `scroll`

목적: 무한 스크롤 목록 페이지에서 카드 링크를 모은 뒤 상세 글을 수집합니다. 기본 selector는 오늘의집 계열에 맞춰져 있습니다.

ChromeDriver 필요 여부: 불필요. `chromiumoxide`가 Chrome/CDP를 직접 사용합니다.

기본 실행:

```powershell
cargo run --bin naver_crawler_engine -- scroll --url "https://ohouse.example/list" --max-posts 50 --workers 3 --out-dir out
```

목록 카드 selector를 직접 지정합니다.

```powershell
cargo run --bin naver_crawler_engine -- scroll --url "https://example.com/list" --card-selector "article.card" --link-selector "a" --max-posts 100 --workers 2 --scroll-pause 1500 --out-dir out
```

로그인 쿠키가 필요하면 JSON 쿠키 파일을 넘깁니다.

```powershell
cargo run --bin naver_crawler_engine -- scroll --url "https://example.com/list" --cookie-file .\cookies.json --max-posts 50 --workers 2 --out-dir out
```

출력:

```text
out\results.csv
out\comments.csv
```

## 4. Plan D: `scrape`

목적: DCInside 목록 페이지를 CDP로 수집합니다.

ChromeDriver 필요 여부: 불필요. CDP 기반으로 Chrome을 직접 띄웁니다.

기본 실행:

```powershell
cargo run --bin naver_crawler_engine -- scrape --url "https://gall.dcinside.com/board/lists/?id=toeic" --max-posts 100 --workers 2 --out-dir out
```

차단/불안정이 보이면 worker를 낮춥니다.

```powershell
cargo run --bin naver_crawler_engine -- scrape --url "https://gall.dcinside.com/board/lists/?id=toeic" --max-posts 50 --workers 1 --out-dir out
```

출력:

```text
out\results.csv
out\comments.csv
```

## 5. Plan E: `smartstore`

목적: 네이버 스마트스토어 상품 리뷰를 병렬 수집합니다.

ChromeDriver 필요 여부: 필요

URL 파일 형식:

```text
https://smartstore.naver.com/store/products/111
https://smartstore.naver.com/store/products/222
```

URL 파일로 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- smartstore --input .\smartstore_urls.txt --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

URL을 직접 여러 개 넘깁니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --url "https://smartstore.naver.com/store/products/111" --url "https://smartstore.naver.com/store/products/222" --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

안정성 우선이면 worker를 1로 낮춥니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --input .\smartstore_urls.txt --workers 1 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

headless로 실행:

```powershell
cargo run --bin naver_crawler_engine -- smartstore --input .\smartstore_urls.txt --workers 2 --webdriver http://localhost:4444 --headless --output out\smartstore_reviews.csv
```

출력:

```text
out\smartstore_reviews.csv
```

## 6. Plan F: `cafe-open`

목적: 네이버 카페 공개/미로그인 접근 가능한 `f-e/cafes/.../menus/...` 목록에서 URL을 먼저 수집하고, 필요하면 상세 글까지 수집합니다.

ChromeDriver 또는 GeckoDriver 필요 여부: 필요

URL만 먼저 모읍니다. 대량 작업 전에는 이 모드가 제일 안전합니다.

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-posts 500 --list-workers 5 --url-only --webdriver http://localhost:4444 --out-dir out
```

URL 수집과 상세 수집을 한 번에 합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-posts 500 --list-workers 5 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

페이지 수를 직접 지정합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 20 --list-workers 4 --url-only --webdriver http://localhost:4444 --out-dir out
```

이미 저장된 URL CSV에서 일부 행만 상세 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url-csv "out\cafe_open_rows_001-500_20260525_120000_urls.csv" --from-row 1 --to-row 50 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

Firefox/GeckoDriver로 실행합니다.

```powershell
.\geckodriver.exe --port 4444
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 10 --list-workers 1 --url-only --browser firefox --webdriver http://localhost:4444 --out-dir out
```

GeckoDriver 포트를 여러 개 열고 목록 수집 worker에 분산합니다.

```powershell
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4444"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4445"
Start-Process -FilePath ".\geckodriver.exe" -ArgumentList "--port","4446"

cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50" --max-pages 100 --list-workers 3 --url-only --browser firefox --webdriver http://localhost:4444 --webdriver http://localhost:4445 --webdriver http://localhost:4446 --out-dir out
```

출력 파일은 실행 시각과 행 범위가 붙습니다.

```text
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_urls.csv
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_results.csv
out\cafe_open_rows_001-500_YYYYMMDD_HHMMSS_comments.csv
```

## 7. Plan F helper: `cafe-menu-commands`

목적: 카페 사이드바 메뉴 목록을 읽어서 메뉴별 `cafe-open` 실행 스크립트를 자동 생성합니다.

ChromeDriver 또는 GeckoDriver 필요 여부: 필요

기본 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- cafe-menu-commands --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0" --max-posts 50000 --list-workers 10 --webdriver http://localhost:4444 --out-dir out
```

각 메뉴의 최대 페이지 수를 지정합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-menu-commands --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0" --max-pages 57 --list-workers 10 --webdriver http://localhost:4444 --out-dir out
```

출력 스크립트 이름을 직접 지정합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-menu-commands --url "https://cafe.naver.com/f-e/cafes/17902534/menus/0" --max-posts 50000 --list-workers 10 --webdriver http://localhost:4444 --out-dir out --output out\run_cafe_menus.ps1
```

생성된 스크립트를 실행합니다.

```powershell
.\out\run_cafe_menus.ps1
```

기본 출력:

```text
out\cafe_menu_commands_YYYYMMDD_HHMMSS.ps1
out\cafe_menu_urls_deduped_YYYYMMDD_HHMMSS.csv
```

## 8. Plan G: `reddit`

목적: Reddit 공개 JSON API로 게시글과 댓글을 수집합니다.

ChromeDriver 필요 여부: 불필요

특정 subreddit 최신글:

```powershell
cargo run --bin naver_crawler_engine -- reddit --subreddit minimalism --sort new --limit 100 --max-pages 3 --max-comments 200 --workers 5 --out-dir out
```

Reddit 전체 검색:

```powershell
cargo run --bin naver_crawler_engine -- reddit --search-query "rust crawler" --sort relevance --limit 100 --max-pages 2 --max-comments 100 --workers 5 --out-dir out
```

키워드 필터를 여러 개 적용합니다. 제목+본문에 키워드가 있는 게시글만 남깁니다.

```powershell
cargo run --bin naver_crawler_engine -- reddit --subreddit all --search-query "naver" --keyword crawler --keyword rust --limit 100 --max-pages 2 --max-comments 100 --workers 5 --user-agent "windows:crawler-rs:v1.0 (by /u/yourname)" --out-dir out
```

출력:

```text
out\reddit_posts.csv
out\reddit_comments.csv
```

## 9. Plan H: `blog-search`

목적: 네이버 블로그 검색에서 키워드와 기간으로 글을 찾고, 본문과 댓글을 수집합니다.

ChromeDriver 필요 여부: 필요

기본 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- blog-search --query "바퀴벌레" --start-date 2026-05-01 --end-date 2026-05-25 --max-posts 30 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

검색 결과를 더 많이 스크롤하고 상세 페이지도 더 많이 스크롤합니다.

```powershell
cargo run --bin naver_crawler_engine -- blog-search --query "바퀴벌레" --start-date 2026-05-01 --end-date 2026-05-25 --max-posts 100 --workers 3 --webdriver http://localhost:4444 --out-dir out --search-max-scrolls 80 --detail-max-scrolls 20
```

출력:

```text
out\<query>_<start>_<end>_posts.csv
out\<query>_<start>_<end>_comments.csv
```

## 10. Plan I: `threads`

목적: Threads 키워드 검색 결과와 댓글을 수집합니다.

ChromeDriver 필요 여부: 필요

기본 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- threads --keyword "바퀴벌레" --max-posts 30 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

댓글 스크롤을 더 많이 합니다.

```powershell
cargo run --bin naver_crawler_engine -- threads --keyword "바퀴벌레" --max-posts 100 --workers 3 --webdriver http://localhost:4444 --out-dir out --comment-scroll-rounds 20 --comment-pause-secs 2
```

출력:

```text
out\threads_<keyword>.csv
out\threads_<keyword>.xlsx
```

## 11. Plan J: `amazon`

목적: Amazon 상품 리뷰를 수집합니다.

ChromeDriver 필요 여부: 필요

처음 실행은 브라우저 로그인/쿠키 저장이 필요할 수 있습니다.

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- amazon --url "https://www.amazon.com/product-reviews/ASIN" --max-reviews 100 --workers 3 --webdriver http://localhost:4444 --out-dir amazon_output
```

URL 파일로 실행:

```powershell
cargo run --bin naver_crawler_engine -- amazon --input .\amazon_review_urls.txt --max-reviews 200 --workers 3 --webdriver http://localhost:4444 --out-dir amazon_output
```

저장된 쿠키 파일을 사용합니다.

```powershell
cargo run --bin naver_crawler_engine -- amazon --input .\amazon_review_urls.txt --max-reviews 200 --workers 3 --webdriver http://localhost:4444 --cookie-file amazon_output\cookies.json --out-dir amazon_output
```

Read More 클릭을 끕니다.

```powershell
cargo run --bin naver_crawler_engine -- amazon --url "https://www.amazon.com/product-reviews/ASIN" --max-reviews 100 --workers 2 --webdriver http://localhost:4444 --no-read-more --out-dir amazon_output
```

출력:

```text
amazon_output\amazon_reviews.csv
```

## 12. Plan K: `goodreads`

목적: Goodreads 리뷰 페이지를 무한 스크롤하며 리뷰를 수집합니다.

ChromeDriver 필요 여부: 필요

기본 실행:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- goodreads --url "https://www.goodreads.com/book/show/BOOK_ID" --workers 3 --webdriver http://localhost:4444 --max-reviews 300 --out-dir goodreads_output
```

URL 파일로 실행:

```powershell
cargo run --bin naver_crawler_engine -- goodreads --input .\goodreads_urls.txt --workers 3 --webdriver http://localhost:4444 --max-reviews 300 --out-dir goodreads_output
```

로그인 세션 프로필과 쿠키 파일을 지정합니다.

```powershell
cargo run --bin naver_crawler_engine -- goodreads --input .\goodreads_urls.txt --workers 2 --webdriver http://localhost:4444 --profile-dir goodreads_profile --cookie-file goodreads_output\cookies.json --max-reviews 0 --max-idle-rounds 5 --out-dir goodreads_output
```

출력:

```text
goodreads_output\goodreads_reviews.csv
```

## 13. Plan L: `instagram` 별도 바이너리

목적: Instagram hashtag/keyword 기준으로 게시글 URL, 본문, 댓글을 수집합니다.

ChromeDriver 필요 여부: 자동으로 chromedriver 프로세스를 재시작하는 로직이 있습니다. 기본 WebDriver URL은 `http://localhost:4444`입니다.

환경 변수 파일 `.env` 예시:

```text
IG_USERNAME=your_instagram_id
IG_PASSWORD=your_instagram_password
WEBDRIVER_URL=http://localhost:4444
HEADLESS=false
```

키워드 파일 `keywords.txt` 예시:

```text
cockroach
naver
rustcrawler
```

URL만 먼저 수집:

```powershell
cargo run --bin instagram -- --input .\keywords.txt --output-dir out\instagram --max-posts 100 --workers 1 --urls-only
```

이미 수집한 URL 파일에서 상세 수집:

```powershell
cargo run --bin instagram -- --input .\keywords.txt --output-dir out\instagram --max-posts 100 --max-comments 200 --min-comment-len 2 --workers 1 --from-urls
```

한 번에 URL과 상세를 수집:

```powershell
cargo run --bin instagram -- --input .\keywords.txt --output-dir out\instagram --max-posts 50 --max-comments 100 --workers 1
```

JSON config를 씁니다.

```powershell
cargo run --bin instagram -- --config .\instagram_config.json --input .\keywords.txt --output-dir out\instagram --max-posts 50 --workers 1
```

출력:

```text
out\instagram\<keyword>_urls.txt
out\instagram\<keyword>_insta.csv
out\instagram\<keyword>_comments.csv
```

## 14. Plan M: `itda-community`

목적: 잇다 커뮤니티 목록과 글 상세를 수집합니다.

ChromeDriver 필요 여부: 필요

처음 실행하면 로그인용 Chrome이 열릴 수 있습니다. 로그인 후 터미널에서 Enter를 누르면 계속 진행합니다.

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 2 --workers 3 --webdriver http://localhost:4444 --out-dir out --profile-dir target\itda_login_profile
```

전체 페이지를 길게 수집:

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 43 --max-posts 0 --workers 3 --webdriver http://localhost:4444 --out-dir out --profile-dir target\itda_login_profile
```

특정 개수까지만 수집:

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 43 --max-posts 100 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

출력:

```text
out\itda_community.csv
```

## 15. Plan N: `naver-search`

목적: 네이버 통합 검색 결과에서 네이버 블로그/Tistory URL을 모으고 본문과 댓글을 수집합니다.

ChromeDriver 필요 여부: 필요

소량 테스트:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=%EB%B0%94%ED%80%B4%EB%B2%8C%EB%A0%88" --max-posts 10 --max-scrolls 5 --workers 2 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 20
```

대량 수집:

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=%EB%B0%94%ED%80%B4%EB%B2%8C%EB%A0%88" --max-posts 0 --max-scrolls 80 --workers 3 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 50
```

댓글을 많이 펼치지 않게 제한:

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=test" --max-posts 30 --max-scrolls 10 --workers 2 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 5
```

출력:

```text
out\naver_search_posts.csv
out\naver_search_comments.csv
```

## 16. Plan O: `coupang`

목적: Coupang 상품 리뷰 API 또는 브라우저 fetch 모드로 리뷰를 수집합니다.

ChromeDriver 필요 여부:

- 기본 API 모드: 불필요
- `--browser-fetch`: 필요

기본 API 모드:

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 3 --output out\coupang_reviews_001_010.csv
```

11~20페이지 이어서 수집:

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 11 --max-pages 10 --workers 3 --output out\coupang_reviews_011_020.csv
```

URL 파일로 실행:

```powershell
cargo run --bin naver_crawler_engine -- coupang --input .\coupang_urls.txt --start-page 1 --max-pages 10 --workers 3 --output out\coupang_reviews.csv
```

Cookie 헤더를 직접 넘깁니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 1 --cookie "PCID=...; sid=..." --page-delay-ms 1500 --output out\coupang_reviews.csv
```

Cookie 파일을 씁니다.

```powershell
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 1 --cookie-file .\coupang_cookie.txt --page-delay-ms 1500 --output out\coupang_reviews.csv
```

403이 계속 나면 브라우저 fetch 모드:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- coupang --url "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327" --start-page 1 --max-pages 10 --workers 1 --cookie-file .\coupang_cookie.txt --browser-fetch --webdriver http://localhost:4444 --page-delay-ms 1500 --output out\coupang_reviews.csv
```

출력:

```text
out\coupang_reviews.csv
```

## 17. Plan P: `kin`

목적: 네이버 지식iN 검색 결과에서 상세 질문 URL을 먼저 모은 뒤, 질문 상세만 수집합니다. 답변은 수집하지 않습니다.

ChromeDriver 필요 여부: 불필요. HTTP로 수집합니다.

가장 기본 실행:

```powershell
cargo run --bin naver_crawler_engine -- kin --url "https://kin.naver.com/search/list.naver?query=%EB%B0%94%ED%80%B4%EB%B2%8C%EB%A0%88" --max-pages 10 --max-posts 100 --workers 3 --out-dir out
```

2페이지부터 5페이지를 수집:

```powershell
cargo run --bin naver_crawler_engine -- kin --url "https://kin.naver.com/search/list.naver?query=%EB%B0%94%ED%80%B4%EB%B2%8C%EB%A0%88" --start-page 2 --max-pages 5 --max-posts 50 --workers 3 --out-dir out
```

차단이 의심되면 딜레이를 늘립니다.

```powershell
cargo run --bin naver_crawler_engine -- kin --url "https://kin.naver.com/search/list.naver?query=%EB%B0%94%ED%80%B4%EB%B2%8C%EB%A0%88" --max-pages 20 --max-posts 200 --workers 1 --page-delay-ms 1500 --detail-delay-ms 1000 --out-dir out
```

출력:

```text
out\kin_search_results.csv
out\kin_questions.csv
```

`kin_search_results.csv`에는 검색 결과 목록의 제목, URL, 날짜, 요약, 카테고리, 답변수, 추천수, 썸네일 URL이 들어갑니다.

`kin_questions.csv`에는 상세 질문의 제목, 작성자, 작성일, 조회수, 카테고리, 질문 본문, 질문 이미지 URL JSON이 들어갑니다. 답변 본문은 의도적으로 저장하지 않습니다.

## 18. `test`

목적: 단일 URL에 대해 사전 점검을 합니다.

ChromeDriver가 필요 없는 점검:

```powershell
cargo run --bin naver_crawler_engine -- test --url "https://blog.naver.com/example/223000000000" --out-dir out
```

WebDriver까지 같이 점검:

```powershell
.\chromedriver.exe --port=4444
cargo run --bin naver_crawler_engine -- test --url "https://blog.naver.com/example/223000000000" --webdriver http://localhost:4444 --out-dir out
```

## 19. 자주 조정하는 옵션

`--workers`는 상세 페이지를 동시에 처리하는 개수입니다. 빠르게 돌리고 싶으면 키우고, 차단/타임아웃이 보이면 1~2로 낮춥니다.

`--list-workers`는 `cafe-open`에서 목록 페이지 URL 수집에 쓰는 worker 수입니다. 상세 수집 worker인 `--workers`와 다릅니다.

`--max-posts 0`은 보통 제한 없음이라는 뜻입니다. 단, Plan마다 실제 종료 조건은 페이지 끝, 스크롤 안정화, idle round 등에 따라 다릅니다.

`--max-pages`는 페이지 번호를 직접 넘기는 방식의 Plan에서 몇 페이지를 볼지 정합니다. `kin`, `coupang`, `cafe-open`, `itda-community`에서 자주 씁니다.

`--out-dir`는 결과 폴더입니다. 단, `smartstore`와 `coupang`은 `--output`으로 CSV 파일명을 직접 받습니다.

`--cookie-file`은 사이트별로 형식이 다를 수 있습니다. 네이버 계열은 보통 JSON cookie array, 쿠팡은 raw Cookie 헤더 또는 JSON cookie array를 받습니다.

PowerShell에서 URL에 `&`가 들어가면 반드시 URL 전체를 큰따옴표로 감싸세요.

## 20. Git commit / push

현재 브랜치 확인:

```powershell
git branch --show-current
```

현재 이 작업 기준으로 올릴 파일:

```powershell
git add README.md src/main.rs src/plan_p.rs
```

커밋:

```powershell
git commit -m "feat: add kin crawler and command docs"
```

현재 브랜치는 `main`, remote는 `origin`입니다. 그대로 푸쉬:

```powershell
git push origin main
```

브랜치가 main이 아닐 때는 현재 브랜치 그대로 upstream을 잡아서 푸쉬합니다.

```powershell
git push -u origin HEAD
```
