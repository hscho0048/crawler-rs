# naver_crawler_engine

Rust 기반 멀티플랜 웹 크롤러. 사이트 특성에 맞게 플랜을 선택해 사용한다.

## 플랜 개요

| 플랜 | 방식 | 대상 사이트 | ChromeDriver 필요 |
|------|------|------------|:-----------------:|
| **A** | HTTP 요청 + HTML 파싱 | 네이버 블로그 (공개) | X |
| **B** | WebDriver (thirtyfour) | 네이버 카페 (가입, 로그인 필요) | O |
| **C** | CDP (chromiumoxide) 무한 스크롤 | 오늘의집 등 스크롤 피드형 | X |
| **D** | CDP (chromiumoxide) 페이지네이션 | DC인사이드 등 게시판형 | X |
| **E** | WebDriver 병렬 Worker Pool | 스마트스토어 상품 리뷰 | O |
| **F** | WebDriver + 네이버 검색 경유 | 네이버 카페 (미가입) | O |
| **G** | reqwest 공개 JSON API | Reddit 서브레딧 | X |
| **H** | WebDriver 병렬 Worker Pool | 네이버 블로그 검색 (키워드+기간) | O |
| **I** | WebDriver 병렬 Worker Pool | Threads.com 키워드 검색 (로그인 필요) | O |
| **J** | WebDriver 병렬 Worker Pool | Amazon 상품 리뷰 (로그인 필요, 쿠키 재사용) | O |
| **K** | WebDriver 병렬 Worker Pool | Goodreads 도서 리뷰 (로그인 필요, 쿠키 재사용) | O |

> Plan C / D는 ChromeDriver 없이 Chrome에 직접 CDP로 연결한다. Chrome 설치만 필요.
> Plan G는 Reddit 공개 JSON API를 사용하므로 Chrome, ChromeDriver, 계정 모두 불필요.

---

## 빌드 요구사항

- Rust stable (2021 edition)
- Chrome 브라우저 (Plan C / D)
- ChromeDriver (Plan B / E / F / H / I, Chrome 버전과 일치해야 함)

```
cargo build --release
```

---

## 출력 파일

모든 플랜 공통 (`--out-dir`로 지정한 디렉토리):

| 파일 | 내용 |
|------|------|
| `results.csv` | 게시글 목록 (`제목`, `url`, `날짜`, `본문`, `댓글`) |
| `comments.csv` | 댓글 상세 (`post_url`, `comment_id`, `is_reply`, `author`, `date`, `content`) |

인코딩: **UTF-8 BOM** (Excel 한글 호환)

---

## Plan A — 네이버 블로그 HTTP 크롤링

공개 블로그 URL 목록을 고속 병렬 HTTP로 수집한다. JavaScript 렌더링이 필요한 페이지는 Plan B로 자동 fallback된다.

### 사용 대상
- `https://m.blog.naver.com/...`
- `https://blog.naver.com/...`

### 명령어

```
cargo run --release -- crawl --input ./urls.txt --max-in-flight 200 --out-dir ./out

cargo run --release -- crawl --url "https://m.blog.naver.com/foo/123" --out-dir ./out

cargo run --release -- crawl --input ./urls.txt --max-in-flight 200 --webdriver http://localhost:4444 --plan-b-pages 5 --out-dir ./out
```

### urls.txt 형식

```
# 주석은 # 으로 시작
https://m.blog.naver.com/foo/111
https://m.blog.naver.com/foo/222
```

---

## Plan B — 네이버 카페 크롤링 (가입 카페)

ChromeDriver + thirtyfour로 실제 브라우저를 제어한다. 로그인 쿠키를 주입해 가입 카페의 회원 전용 게시글도 수집 가능하다.

### 사전 준비

```
chromedriver.exe --port=4444
```

### 쿠키 파일 형식 (`cookies.json`)

```json
[
  {"name": "NID_AUT", "value": "..."},
  {"name": "NID_SES", "value": "..."}
]
```

> 브라우저 개발자도구 → Application → Cookies → `cafe.naver.com` 에서 복사

### 명령어

```
cargo run --release -- cafe --url "https://cafe.naver.com/ArticleList.nhn?search.clubid=12345&search.boardtype=L" --max-posts 50 --workers 3 --webdriver http://localhost:4444 --out-dir ./out

cargo run --release -- cafe --url "https://cafe.naver.com/ArticleList.nhn?search.clubid=12345&search.boardtype=L" --max-posts 50 --workers 3 --webdriver http://localhost:4444 --cookie-file ./cookies.json --out-dir ./out
```

---

## Plan C — 무한 스크롤 사이트 CDP 크롤링

ChromeDriver 없이 CDP로 Chrome을 직접 제어한다. 스크롤로 게시글이 로드되는 피드형 사이트에 사용한다.

### 사용 대상
- 오늘의집 (`https://ohou.se/cards/feed`)

### 명령어

```
cargo run --release -- scroll --url "https://ohou.se/cards/feed" --max-posts 30 --workers 3 --out-dir ./out

cargo run --release -- scroll --url "https://example.com/feed" --max-posts 50 --workers 3 --card-selector "div.post-card" --link-selector "a.post-link" --scroll-pause 2000 --out-dir ./out

cargo run --release -- scroll --url "https://ohou.se/cards/feed" --max-posts 30 --workers 3 --cookie-file ./cookies.json --out-dir ./out
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--max-posts` | 20 | 수집할 최대 게시글 수 |
| `--workers` | 3 | 동시 탭(페이지) 수 |
| `--card-selector` | `article.css-71vdks` | 게시글 카드 CSS 셀렉터 |
| `--link-selector` | `a` | 카드 내 링크 셀렉터 |
| `--scroll-pause` | 1500 | 스크롤 후 대기 시간 (ms) |

---

## Plan D — 페이지네이션 게시판 CDP 크롤링

ChromeDriver 없이 CDP로 Chrome을 직접 제어한다. `?page=N` 방식으로 페이지를 순회하는 게시판에 사용한다.

### 사용 대상
- DC인사이드 (`https://gall.dcinside.com/board/lists/?id=...`)

### 명령어

```
cargo run --release -- scrape --url "https://gall.dcinside.com/board/lists/?id=toeic" --max-posts 50 --workers 2 --out-dir ./out
```

> DC인사이드는 IP 차단이 있으므로 `--workers`는 2~3을 권장한다.

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--max-posts` | 100 | 수집할 최대 게시글 수 |
| `--workers` | 2 | 병렬 처리 워커 수 |

---

## Plan E — 스마트스토어 리뷰 수집 (병렬 Worker Pool)

WebDriver + thirtyfour로 스마트스토어 상품의 리뷰를 수집한다.
여러 상품을 `workers`개의 Chrome 세션이 병렬로 처리한다.

### 사전 준비

```
chromedriver.exe --port=4444
```

### 명령어

```
cargo run --release -- smartstore --url "https://smartstore.naver.com/store1/products/111" --url "https://smartstore.naver.com/store2/products/222" --workers 3 --webdriver http://localhost:4444 --output out/reviews.csv

cargo run --release -- smartstore --input ./product_urls.txt --workers 3 --webdriver http://localhost:4444 --output out/reviews.csv

cargo run --release -- smartstore --input ./product_urls.txt --workers 2 --webdriver http://localhost:4444 --output out/reviews.csv --headless
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--url` | — | 상품 URL (반복 사용 가능) |
| `--input` | — | URL 목록 파일 (한 줄에 하나) |
| `--workers` | 2 | 병렬 Chrome 세션 수 |
| `--webdriver` | — | ChromeDriver 엔드포인트 (필수) |
| `--output` | `out/smartstore_reviews.csv` | 결과 CSV 저장 경로 |
| `--headless` | false | 헤드리스 모드 |

### 출력 CSV 컬럼

| 컬럼 | 설명 |
|------|------|
| `product_url` | 상품 URL |
| `page` | 리뷰 페이지 번호 |
| `idx_in_page` | 페이지 내 순서 |
| `review` | 리뷰 본문 |
| `rating` | 별점 (1.0 ~ 5.0) |
| `date` | 작성일 |
| `raw_text` | 원본 텍스트 (디버그용) |

### product_urls.txt 형식

```
# 주석은 # 으로 시작
https://smartstore.naver.com/store1/products/111
https://smartstore.naver.com/store2/products/222
```

---

## Plan F — 미가입 네이버 카페 크롤링

가입하지 않아도 열람 가능한 카페 게시글을 수집한다.
네이버 검색을 경유해 공개 접근 URL을 얻고, Plan B의 스크래퍼로 본문·댓글을 추출한다.

### 동작 흐름

1. Plan B의 목록 수집 로직으로 게시글 URL·제목 수집
2. 각 게시글 제목으로 네이버 카페 탭 검색 → 검색 결과 URL로 교체
3. 교체된 URL로 Plan B 스크래퍼 호출 → 본문·댓글 추출

> 검색 결과가 없거나 매칭 실패 시 해당 게시글은 건너뛴다.

### 사전 준비

```
chromedriver.exe --port=4444
```

### 명령어

```
cargo run --release -- cafe-open \
  --url "https://cafe.naver.com/cafename/board" \
  --max-posts 50 \
  --workers 3 \
  --webdriver http://localhost:4444 \
  --out-dir ./out
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--url` | — | 카페 게시판 URL (필수) |
| `--max-posts` | 20 | 수집할 최대 게시글 수 |
| `--workers` | 3 | 병렬 Chrome 세션 수 |
| `--webdriver` | — | ChromeDriver 엔드포인트 (필수) |
| `--out-dir` | `out` | 결과 저장 디렉토리 |

### Plan B와의 차이

| | Plan B | Plan F |
|---|---|---|
| 로그인 쿠키 | 필요 | 불필요 |
| 접근 방식 | 직접 URL | 네이버 검색 경유 |
| 수집 범위 | 회원 전용 포함 | 공개 게시글만 |

---

## Plan G — Reddit 서브레딧 크롤링

Reddit 공개 JSON API를 사용한다. Chrome, ChromeDriver, 계정 모두 불필요.
전체 피드 수집(`/r/{subreddit}/{sort}.json`)과 서브레딧 내 키워드 검색(`/r/{subreddit}/search.json`) 두 가지 모드를 지원한다.

### 출력 파일

| 파일 | 내용 |
|------|------|
| `reddit_posts.csv` | 게시글 (제목, 본문, 작성자, 점수, URL 등) |
| `reddit_comments.csv` | 댓글 전체 (대댓글 포함, depth 컬럼으로 구분) |

인코딩: **UTF-8 BOM** (Excel 한글 호환)

### 한 줄 실행

```
cargo run --release -- reddit --subreddit running --search-query "Nike" --sort relevance --out-dir ./out
```

### 명령어

```
# 전체 피드 수집 (최신순 3페이지)
cargo run --release -- reddit --subreddit minimalism --sort new --max-pages 3 --out-dir ./out

# 서브레딧 내 키워드 검색 (Reddit 검색 API)
cargo run --release -- reddit --subreddit running --search-query "Nike" --sort relevance --max-pages 5 --out-dir ./out

# 검색 + 클라이언트 필터 조합
cargo run --release -- reddit --subreddit running --search-query "Nike" --keyword "shoe" --keyword "review" --out-dir ./out

# 인기순, 댓글 대량 수집
cargo run --release -- reddit --subreddit malefashionadvice --sort hot --max-pages 5 --max-comments 500 --workers 5 --out-dir ./out
```

### 검색 모드 비교

| | `--search-query` | `--keyword` |
|---|---|---|
| 방식 | Reddit 검색 API 호출 | 수집 후 클라이언트 필터링 |
| 속도 | 빠름 (서버 필터) | 느림 (전체 수집 후 필터) |
| `--sort` 옵션 | `relevance` 권장 | `new` / `hot` 등 |

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--subreddit` | — | 서브레딧 이름 (필수) |
| `--search-query` | — | 서브레딧 내 검색어 (Reddit 검색 API) |
| `--sort` | `new` | 정렬 방식 (`new` / `hot` / `top` / `rising` / `relevance`) |
| `--limit` | 100 | 페이지당 최대 게시글 수 (Reddit 최대 100) |
| `--max-pages` | 3 | 최대 페이지 수 |
| `--max-comments` | 200 | 게시글당 최대 댓글 수 |
| `--keyword` | — | 클라이언트 필터 (반복 사용 가능, 없으면 전체) |
| `--workers` | 5 | 댓글 병렬 수집 동시성 |
| `--page-delay-ms` | 2000 | 페이지 요청 사이 딜레이 (ms) |
| `--user-agent` | `rust:reddit-crawler:v1.0 (by /u/anonymous)` | HTTP User-Agent |
| `--out-dir` | `out` | 결과 저장 디렉토리 |

### Rate Limit 대응

Reddit 공개 API는 요청이 많으면 `429 Too Many Requests`를 반환한다.

- `Retry-After` 헤더가 있으면 그 시간(초)만큼 자동 대기 후 재시도 (없으면 120초)
- `--page-delay-ms 3000` 이상으로 설정하면 429 빈도가 줄어든다
- `--workers`를 낮추면 댓글 수집 동시 요청 수가 줄어든다

---

## Plan H — 네이버 블로그 검색 크롤링

키워드와 기간을 지정해 네이버 블로그 검색 결과를 수집한다.
1단계에서 검색 결과 전체를 스크롤해 URL 목록을 모은 뒤, 2단계에서 `workers`개의 Chrome 세션이 본문·댓글을 병렬 수집한다.

### 사전 준비

```
chromedriver.exe --port=9515
```

> ChromeDriver 포트 기본값이 **9515**임에 주의 (다른 플랜은 4444).

### 한 줄 실행

```
cargo run --release -- blog-search --query "제주도 맛집" --start-date 2025-01-01 --end-date 2025-03-01 --webdriver http://localhost:9515 --out-dir ./out
```

### 명령어

```
# 기본 (헤드리스, 워커 3개, 최대 30개)
cargo run --release -- blog-search --query "제주도 맛집" --start-date 2025-01-01 --end-date 2025-03-01 --webdriver http://localhost:9515 --out-dir ./out

# 대량 수집 (워커 5개, 최대 200개)
cargo run --release -- blog-search --query "다이어트 식단" --start-date 2024-06-01 --end-date 2024-12-31 --max-posts 200 --workers 5 --webdriver http://localhost:9515 --out-dir ./out

# 브라우저 창 보이게 (디버그용)
cargo run --release -- blog-search --query "러닝화 추천" --start-date 2025-01-01 --end-date 2025-03-01 --webdriver http://localhost:9515 --headless false --out-dir ./out
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--query` | — | 검색 키워드 (필수) |
| `--start-date` | — | 검색 시작일 `YYYY-MM-DD` (필수) |
| `--end-date` | — | 검색 종료일 `YYYY-MM-DD` (필수) |
| `--max-posts` | 30 | 수집할 최대 게시글 수 |
| `--workers` | 3 | 병렬 Chrome 세션 수 |
| `--webdriver` | `http://localhost:9515` | ChromeDriver 엔드포인트 |
| `--headless` | true | 헤드리스 모드 (false 시 브라우저 창 표시) |
| `--out-dir` | `out` | 결과 저장 디렉토리 |
| `--search-max-scrolls` | 30 | 검색 결과 최대 스크롤 횟수 |
| `--detail-max-scrolls` | 8 | 게시글 페이지 최대 스크롤 횟수 |

### 출력 파일

`{키워드}_{시작일}_{종료일}_posts.csv` / `_comments.csv` 형태로 저장된다.

**posts CSV**

| 제목 | url | 날짜 | 본문 | 댓글 |
|------|-----|------|------|------|
| 게시글 제목 | 블로그 포스트 URL | 검색 결과 표시 날짜 | 본문 전체 텍스트 | 댓글 JSON 배열 |

**comments CSV**

| 컬럼 | 설명 |
|------|------|
| `post_url` | 게시글 URL |
| `comment_id` | 댓글 ID |
| `parent_comment_id` | 부모 댓글 ID (대댓글인 경우) |
| `reply_level` | 댓글 깊이 (1: 원댓글, 2: 대댓글) |
| `author_name` | 작성자 닉네임 |
| `content` | 댓글 내용 |
| `created_at` | 작성 일시 |
| `like_count` | 좋아요 수 |

인코딩: **UTF-8 BOM** (Excel 한글 호환)

---

## Plan I — Threads.com 키워드 크롤링

키워드로 Threads 검색 결과를 수집한다. 로그인이 필요하므로 1단계에서 브라우저 창을 열어 수동 로그인 후 Enter를 누르면, 2단계에서 `workers`개의 헤드리스 Chrome이 게시글·댓글을 병렬 수집한다.

### 동작 흐름

1. 브라우저 창 열림 → `https://www.threads.com` 이동
2. **수동 로그인 후 Enter 입력**
3. 검색 URL로 이동 → 무한 스크롤로 게시글 URL 수집
4. 쿠키 추출 → 1단계 드라이버 종료
5. N개 헤드리스 드라이버가 쿠키 주입 후 게시글 상세·댓글 병렬 수집
6. CSV + XLSX 저장

### 사전 준비

```
chromedriver.exe --port=9515
```

### 한 줄 실행

```
cargo run --release -- threads --keyword "러닝화" --webdriver http://localhost:9515 --out-dir ./out
```

### 명령어

```
# 기본 (워커 3개, 최대 30개)
cargo run --release -- threads --keyword "러닝화" --webdriver http://localhost:9515 --out-dir ./out

# 대량 수집 (워커 5개, 최대 100개)
cargo run --release -- threads --keyword "다이어트" --max-posts 100 --workers 5 --webdriver http://localhost:9515 --out-dir ./out

# 댓글 스크롤 조정 (댓글 많은 게시글)
cargo run --release -- threads --keyword "맛집" --comment-scroll-rounds 20 --comment-pause-secs 2 --webdriver http://localhost:9515 --out-dir ./out
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--keyword` | — | 검색 키워드 (필수) |
| `--max-posts` | 30 | 수집할 최대 게시글 수 |
| `--workers` | 3 | 병렬 Chrome 세션 수 |
| `--webdriver` | `http://localhost:9515` | ChromeDriver 엔드포인트 |
| `--out-dir` | `out` | 결과 저장 디렉토리 |
| `--comment-scroll-rounds` | 10 | 댓글 페이지 스크롤 최대 횟수 |
| `--comment-pause-secs` | 1 | 댓글 스크롤 간격 (초) |

### 출력 파일

`threads_{키워드}.csv` / `.xlsx` 형태로 저장된다. 댓글이 있는 게시글은 댓글 수만큼 행이 반복된다.

| 컬럼 | 설명 |
|------|------|
| `keyword` | 검색 키워드 |
| `url` | 게시글 URL |
| `author` | 작성자 |
| `date` | 작성 일시 |
| `post_text` | 게시글 본문 |
| `likes` | 좋아요 수 |
| `replies` | 댓글 수 |
| `reposts` | 리포스트 수 |
| `comment_text` | 댓글 내용 (댓글 없으면 빈값) |

CSV 인코딩: **UTF-8 BOM** (Excel 한글 호환)

> Threads는 로그인 없이는 검색 결과 접근이 제한된다. 1단계 로그인 창에서 반드시 로그인 후 Enter를 눌러야 한다.

---

## Plan J — Amazon 상품 리뷰 수집 (병렬 Worker Pool)

여러 Amazon 상품의 리뷰를 `workers`개의 Chrome 세션이 동시에 수집한다.
각 워커는 상품 하나를 담당해 Next 버튼으로 페이지를 순차 이동하며 목표 리뷰 수까지 수집한다.
첫 실행 시 수동 로그인 후 쿠키를 자동 저장하고, 이후에는 쿠키 파일로 완전 헤드리스 실행이 가능하다.

### 사전 준비

```
chromedriver.exe --port=4444
```

### 동작 흐름

1. `--cookie-file` 없는 경우: 브라우저 창이 열림 → Amazon 로그인 후 Enter → 쿠키 `amazon_output/cookies.json`에 자동 저장
2. 상품 URL 큐에서 워커가 하나씩 가져가 Next 버튼 클릭으로 페이지 순차 수집
3. 목표 리뷰 수 도달 또는 마지막 페이지에서 종료

### 한 줄 실행 (첫 실행, 로그인 필요)

```
cargo run --release -- amazon --url "https://www.amazon.com/product-reviews/ASIN" --max-reviews 100 --workers 3 --webdriver http://localhost:4444
```

### 이후 실행 (완전 헤드리스)

```
cargo run --release -- amazon \
  --url "https://www.amazon.com/product-reviews/ASIN1" \
  --url "https://www.amazon.com/product-reviews/ASIN2" \
  --url "https://www.amazon.com/product-reviews/ASIN3" \
  --max-reviews 200 \
  --workers 3 \
  --headless \
  --cookie-file amazon_output/cookies.json \
  --webdriver http://localhost:4444
```

### URL 파일로 입력

```
cargo run --release -- amazon --input ./asins.txt --max-reviews 200 --workers 3 --headless --cookie-file amazon_output/cookies.json --webdriver http://localhost:4444
```

```
# asins.txt
https://www.amazon.com/product-reviews/B00KK0PICK
https://www.amazon.com/product-reviews/B01A4B2JHG
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--url` | — | 상품 리뷰 URL (반복 사용 가능) |
| `--input` | — | URL 목록 파일 (한 줄에 하나) |
| `--max-reviews` | 100 | 상품당 수집할 최대 리뷰 수 |
| `--workers` | 3 | 병렬 Chrome 세션 수 (동시 수집 상품 수) |
| `--webdriver` | `http://localhost:9515` | ChromeDriver 엔드포인트 |
| `--headless` | false | 헤드리스 모드 (`--cookie-file`과 함께 사용) |
| `--cookie-file` | — | 쿠키 파일 경로 (첫 실행 후 자동 생성) |
| `--no-read-more` | false | Read More 클릭 비활성화 |
| `--out-dir` | `amazon_output` | 결과 저장 디렉토리 |

### 출력 파일

`amazon_output/amazon_reviews.csv` (UTF-8 BOM)

| 컬럼 | 설명 |
|------|------|
| `product_url` | 수집한 페이지 URL |
| `page_number` | 페이지 번호 |
| `product_title` | 상품명 |
| `total_rating` | 전체 평점 |
| `total_review_count` | 전체 리뷰 수 |
| `review_id` | 리뷰 고유 ID |
| `author` | 작성자 |
| `review_title` | 리뷰 제목 |
| `rating` | 별점 |
| `review_country` | 작성 국가 |
| `review_date` | 작성일 |
| `verified_purchase` | 구매 인증 여부 |
| `helpful_votes` | 도움됨 투표 수 |
| `review_text` | 리뷰 본문 |

---

## Plan K — Goodreads 도서 리뷰 수집 (병렬 Worker Pool)

여러 Goodreads 도서의 리뷰를 `workers`개의 Chrome 세션이 동시에 수집한다.
각 워커는 도서 하나를 담당해 무한 스크롤 + "Show more reviews" 버튼으로 목표 리뷰 수까지 수집한다.
첫 실행 시 수동 로그인 후 쿠키를 자동 저장하고, 이후에는 쿠키 파일로 완전 헤드리스 실행이 가능하다.

### 사전 준비

```
chromedriver.exe --port=9515
```

### 쿠키 생성 (최초 1회)

```
cargo run -- goodreads --url "https://www.goodreads.com/book/show/8576972/reviews" --webdriver http://localhost:9515
```

브라우저가 열리면 Goodreads 로그인 → Enter → `goodreads_output/cookies.json` 자동 저장

### 이후 실행 (완전 헤드리스)

```
cargo run -- goodreads --url "https://www.goodreads.com/book/show/8576972/reviews" --url "https://www.goodreads.com/book/show/12345/reviews" --workers 2 --max-reviews 100 --headless --cookie-file goodreads_output/cookies.json --webdriver http://localhost:9515
```

### URL 파일로 입력

```
cargo run -- goodreads --input ./books.txt --workers 3 --max-reviews 200 --headless --cookie-file goodreads_output/cookies.json --webdriver http://localhost:9515
```

```
# books.txt
https://www.goodreads.com/book/show/8576972/reviews
https://www.goodreads.com/book/show/12345/reviews
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--url` | — | 도서 리뷰 URL (반복 사용 가능) |
| `--input` | — | URL 목록 파일 (한 줄에 하나) |
| `--max-reviews` | 0 | 도서당 최대 리뷰 수 (0 = 무제한) |
| `--workers` | 3 | 병렬 Chrome 세션 수 |
| `--webdriver` | `http://localhost:9515` | ChromeDriver 엔드포인트 |
| `--headless` | false | 헤드리스 모드 (`--cookie-file`과 함께 사용) |
| `--cookie-file` | — | 쿠키 파일 경로 (첫 실행 후 자동 생성) |
| `--profile-dir` | `goodreads_profile` | Chrome 프로필 디렉토리 (로그인 단계에서만 사용) |
| `--max-idle-rounds` | 5 | 새 리뷰 없을 때 종료까지 허용 라운드 수 |
| `--out-dir` | `goodreads_output` | 결과 저장 디렉토리 |

### 출력 파일

`goodreads_output/goodreads_reviews.csv` (UTF-8 BOM)

| 컬럼 | 설명 |
|------|------|
| `reviewer` | 작성자 |
| `rating` | 별점 (1~5, 없으면 빈값) |
| `date` | 작성일 |
| `review_url` | 리뷰 고유 URL |
| `review_text` | 리뷰 본문 |

---

## 사전 점검 (Test)

크롤링 전에 출력 디렉토리와 WebDriver 연결을 확인한다.

```
cargo run --release -- test --url "https://m.blog.naver.com/foo/123" --out-dir ./out

cargo run --release -- test --url "https://cafe.naver.com/..." --out-dir ./out --webdriver http://localhost:4444
```

---

## 로그 레벨

```
set RUST_LOG=debug && cargo run -- scroll --url "https://ohou.se/cards/feed" --out-dir ./out
set RUST_LOG=warn  && cargo run -- scrape --url "https://gall.dcinside.com/board/lists/?id=toeic" --out-dir ./out
```
