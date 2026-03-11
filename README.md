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
| **G** | reqwest 공개 JSON API | Reddit 서브레딧 | X |

> Plan C / D는 ChromeDriver 없이 Chrome에 직접 CDP로 연결한다. Chrome 설치만 필요.
> Plan G는 Reddit 공개 JSON API를 사용하므로 Chrome, ChromeDriver, 계정 모두 불필요.

---

## 빌드 요구사항

- Rust stable (2021 edition)
- Chrome 브라우저 (Plan C / D)
- ChromeDriver (Plan B / E, Chrome 버전과 일치해야 함)

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

## Plan G — Reddit 서브레딧 크롤링

Reddit 공개 JSON API(`reddit.com/r/{subreddit}.json`)를 사용한다.
Chrome, ChromeDriver, Reddit 계정 모두 불필요.
게시글 목록을 순차 수집 + 게시글별 댓글을 병렬 수집한다.

### 출력 파일

| 파일 | 내용 |
|------|------|
| `reddit_posts.csv` | 게시글 (제목, 본문, 작성자, 점수, URL 등) |
| `reddit_comments.csv` | 댓글 전체 (대댓글 포함, depth 컬럼으로 구분) |

### 명령어

```
# 기본 수집 (minimalism 서브레딧, 최신순 3페이지)
cargo run --release -- reddit --subreddit minimalism --sort new --max-pages 3 --workers 3 --out-dir ./out

# 키워드 필터 (제목+본문에 단어가 포함된 게시글만 수집)
cargo run --release -- reddit --subreddit minimalism --keyword "capsule wardrobe" --keyword "closet" --out-dir ./out

# 인기순, 댓글 많이
cargo run --release -- reddit --subreddit malefashionadvice --sort hot --max-pages 5 --max-comments 500 --workers 5 --out-dir ./out
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--subreddit` | — | 서브레딧 이름 (필수) |
| `--sort` | `new` | 정렬 방식 (`new` / `hot` / `top` / `rising`) |
| `--limit` | 100 | 페이지당 최대 게시글 수 (Reddit 최대 100) |
| `--max-pages` | 3 | 최대 페이지 수 |
| `--max-comments` | 200 | 게시글당 최대 댓글 수 |
| `--keyword` | — | 키워드 필터 (반복 사용 가능, 없으면 전체) |
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
