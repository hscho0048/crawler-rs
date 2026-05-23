# naver_crawler_engine

Rust 기반 크롤러 모음입니다. ChromeDriver를 켜고 `cargo run --bin naver_crawler_engine -- <명령>` 형태로 실행합니다.

## 준비

ChromeDriver를 먼저 실행합니다.

```powershell
.\chromedriver.exe --port=4444
```

다른 터미널에서 크롤러를 실행합니다.

```powershell
cargo run --bin naver_crawler_engine -- <subcommand> [options]
```

검증용 명령어:

```powershell
cargo check --workspace
cargo test plan_f::tests --workspace
```

## SmartStore 리뷰

URL 파일을 한 줄에 하나씩 넣어 실행합니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --input .\url.txt --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

주소를 직접 여러 개 넘길 수도 있습니다.

```powershell
cargo run --bin naver_crawler_engine -- smartstore --url "https://smartstore.naver.com/store/products/111" --url "https://smartstore.naver.com/store/products/222" --workers 2 --webdriver http://localhost:4444 --output out\smartstore_reviews.csv
```

한 줄씩 안정적으로 돌릴 때는 `--workers 1`을 쓰면 됩니다.

## Plan F: 네이버 카페 공개 접근 크롤러

기존 명령어는 그대로 동작합니다. `--url`로 카페 게시판을 넣으면 URL 목록을 먼저 수집하고, 상세 페이지를 병렬로 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/cafename/board" --max-posts 100 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

특정 행 범위만 수집할 수 있습니다. 행 번호는 수집된 URL 목록 기준 1부터 시작합니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url "https://cafe.naver.com/cafename/board" --max-posts 200 --from-row 51 --to-row 100 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

`--to-row 0`은 마지막 행까지라는 뜻입니다.

### URL CSV만으로 재실행

가능합니다. 이전 실행에서 만들어진 `*_urls.csv`만 있으면 `--url` 없이 상세 수집을 다시 돌릴 수 있습니다.

```powershell
cargo run --bin naver_crawler_engine -- cafe-open --url-csv "out\cafe_open_rows_001-100_20260523_120000_urls.csv" --from-row 1 --to-row 50 --workers 3 --webdriver http://localhost:4444 --out-dir out
```

출력 파일명은 자동으로 바뀝니다.

```text
out\cafe_open_rows_001-100_YYYYMMDD_HHMMSS_urls.csv
out\cafe_open_rows_001-100_YYYYMMDD_HHMMSS_results.csv
out\cafe_open_rows_001-100_YYYYMMDD_HHMMSS_comments.csv
```

`results.csv` 계열 파일의 `comments` 컬럼은 여러 댓글을 ` | `로 합쳐 한 행에 저장합니다. 댓글을 행 단위로 따로 볼 때는 `comments.csv` 계열 파일을 사용합니다.

## Plan M: 잇다 커뮤니티

처음 실행하면 로그인용 Chrome 창이 뜹니다. 로그인 후 터미널에서 Enter를 누르면 `/community?page=1`로 돌아가 URL을 수집하고, 각 글의 날짜, 본문, 댓글을 수집합니다.

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 2 --workers 10 --webdriver http://localhost:4444 --out-dir out
```

옵션:

```powershell
cargo run --bin naver_crawler_engine -- itda-community --start-page 1 --max-pages 43 --max-posts 0 --workers 3 --webdriver http://localhost:4444 --out-dir out --profile-dir target\itda_login_profile
```

출력:

```text
out\itda_community.csv
```

댓글이 여러 개면 `comment_body`에 ` | `로 합쳐 저장합니다.

## Plan N: 네이버 검색 전체 / 블로그 / 티스토리

네이버 검색 URL을 그대로 넣습니다. 검색 결과에서 URL을 수집한 뒤, 네이버 블로그와 티스토리 상세를 병렬로 수집합니다.

테스트로 10개만:

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=..." --max-posts 10 --max-scrolls 5 --workers 2 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 20
```

무한 스크롤로 나온 결과를 최대한 수집하려면 `--max-posts 0`으로 두고 `--max-scrolls`를 크게 잡습니다.

```powershell
cargo run --bin naver_crawler_engine -- naver-search --url "https://search.naver.com/search.naver?ssc=tab.blog.all&query=..." --max-posts 0 --max-scrolls 80 --workers 3 --webdriver http://localhost:4444 --out-dir out --comment-page-limit 50
```

출력:

```text
out\naver_search_posts.csv
out\naver_search_comments.csv
```

`naver_search_posts.csv`의 `comments` 컬럼은 여러 댓글을 ` | `로 합칩니다. 댓글을 별도 행으로 분석할 때는 `naver_search_comments.csv`를 사용합니다.

## 자주 쓰는 옵션

`--workers`는 동시에 띄울 Chrome 세션 수입니다. 한 줄씩 안정적으로 돌릴 때는 `1`, 빠르게 돌릴 때는 `3~10` 사이에서 PC 사양에 맞게 조절합니다.

`--headless`는 브라우저 창 없이 실행하는 옵션입니다. 로그인이나 화면 확인이 필요한 흐름에서는 빼는 편이 편합니다.

Plan F의 페이지 로드 타임아웃은 120초로 설정되어 있습니다.

## 문제 해결

ChromeDriver에서 포트 사용 중 오류가 나면 이미 실행 중인 ChromeDriver가 있는지 확인한 뒤 하나만 남기고 다시 실행합니다.

```powershell
Get-Process chromedriver -ErrorAction SilentlyContinue
```

ChromeDriver와 Chrome 버전이 맞지 않으면 WebDriver 세션 생성이 실패할 수 있습니다. Chrome 버전에 맞는 ChromeDriver를 사용하세요.
