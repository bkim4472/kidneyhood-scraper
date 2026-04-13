#!/usr/bin/env python3
"""Create KidneyHood Amazon Reviews Google Sheet via gws CLI."""

import json, subprocess, sys, re

def gws(*args, params=None, body=None):
    cmd = ["gws"] + list(args)
    if params is not None:
        cmd += ["--params", json.dumps(params, ensure_ascii=False)]
    if body is not None:
        cmd += ["--json", json.dumps(body, ensure_ascii=False)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"gws error ({' '.join(args[:4])}):\n{r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(r.stdout)

def clean(text):
    if not text:
        return ""
    text = (text
        .replace("&#39;", "'").replace("&#34;", '"')
        .replace("&amp;", "&").replace("&#x27;", "'")
        .replace("&#x2F;", "/"))
    # Strip Amazon truncation artifact: "short preview... full text"
    m = re.match(r'^(.{40,350})\.\.\. (.{100,})', text, re.DOTALL)
    if m:
        text = m.group(2)
    return text.replace("\n", " ").strip()

HEADERS = ["Review ID", "Date", "Rating", "Verified Purchase",
           "Helpful Votes", "Title", "Review Text", "Format"]

# ── All scraped reviews ─────────────────────────────────────────────────────

BOOKS = [
    {
        "sheet": "Evidence-Based Guide",
        "asin": "B0DM96NVSX",
        "avg_rating": 4.4,
        "total_ratings": 98,
        "reviews_scraped": 8,
        "dataset_id": "NanNefxJBwFh70JPM",
        "rows": [
            ["R16HS1WDS7OZKG","2026-02-23",5,True,0,"Delivery as requested.,",
             "Excellent information, great research..","Paperback"],
            ["R3MVBKEP41T3TF","2026-01-06",5,True,0,"Great resource",
             "There's so much information in this book everyone with CKD should buy this book.","Paperback"],
            ["R1G1T482FD26I7","2025-12-19",5,True,0,"He has more books of interest",
             "Very informative, I needed that.","Paperback"],
            ["R41FES3HKBUOK","2025-08-31",5,True,6,"Easy to understand and a great guide",
             "I had researched the topic, and the author was very thorough. Easy to read, but best of all the information tracked what my nephrologist recommended already. The information did improve my lab results and will give you the confidence to make better dietary decisions. That goes a long way in staying on a strict kidney friendly diet. My eGFR was dramatically improved over a 6 month period. As always, check with your doctors before making any changes in your treatment plan.",
             "Paperback"],
            ["R27R5B4CA7XXK3","2025-08-16",5,True,6,"Excellent factual guide to KCD.",
             "One of the only true factual guides to chronic Kidney disease Information","Paperback"],
            ["R1MTTP1PPWVKRC","2025-06-16",5,True,11,
             "Get all of his books. His concepts work if you work them.",
             "If you have CKD Lee Hull's books will be a life changer IF you follow his advice. eFGR 52 May 2024. Big surprise to me I'd been 62-70 for the past 4 years my doc never mentioned a thing. I was already in stage 2. In the fall I started seeing a kidney dietician and went on a more plant based diet, she was nice but really not that much help. I saw a nephrologist in December who put me on Farizga (very expensive). I then found Lee's books. Changed my eating plan completely and went from 59 in October to 64 in Feb to 71 in June 6 2025. Stopped the Farziga in Feb since I was getting side effects and did not need it. YES you do need dramatic changes and as someone who also has CKD said to me I don't like vegetables and don't care if I live longer. Well I'm 74 and i want to live till my 90s and see my Grandkids grow up. If no meat and a very low protein diet keeps me off dialysis and healthy it's worth it to me. Like most things it's always up to the individual to make and follow his decisions.",
             "Paperback"],
            ["RVWEF8FKA7D6O","2025-02-28",5,True,6,"Best Kidney advice",
             "This book is good for getting you to a point where you can make a sound decision to get and keep your diet clean and how to do it the right way.","Paperback"],
            ["R37PRJIZBRN6M9","2025-01-12",3,True,23,"Read Stopping Kidney Disease Instead",
             "After reading Lee Hull's longer book, Stopping Kidney Disease, I changed to a very low protein vegan diet and used Albutrix tablets for most of the last 3 years. The Evidence-Based Guide to Kidney and Renal Diets is a shorter book that added nothing new to what I knew. The new feature is a collection of selected case studies. Hull did not analyze the results of all his Albutrix users or take a random sample of his users. He excluded some users, including me, from his study. Readers should note that there is no known way of stopping the progression of chronic kidney disease (CKD). Many nephrologists promote very dangerous drugs to slow the progression because they know that many patients will refuse to change their diets. These drugs have side effects such as death, gangrene, and serious yeast infections. After switching to a vegan diet, there were some early gains: (1) A1C fell from 6.1 to less than 5; no longer pre-diabetic (2) BUN dropped from 26 to as low as 8. BUN decreases because Albutrix tablets contain the lowest nitrogen content of any protein supplement. A problem with a vegan diet is that my red blood cell numbers went from normal to bad. Having had a calcium oxalate kidney stone, I needed to switch also to a low oxalate diet. The result is protein energy wasting. Instead of the Evidence book, read the Stopping book and research also newer findings.",
             "Paperback"],
        ]
    },
    {
        "sheet": "Stopping Kidney Disease",
        "asin": "0692901159",
        "avg_rating": 4.3,
        "total_ratings": 1633,
        "reviews_scraped": 8,
        "dataset_id": "H3LIeUh9UBUjEj1aJ",
        "rows": [
            ["R1GBPRYQ8FZ9WF","2026-03-11",5,True,0,"Reliable book","Good book, information.","Paperback"],
            ["RJMA66UUQN6DP","2021-03-20",5,True,24,"What your doctor won't tell you",
             "I am 82 years old. I was diagnosed with stage 3 kidney disease in September of 2020 after being told for months before that that my falling eGFR was just due to congestive heart failure. After a week in the hospital in mid-October for what apparently was a multiple failure of body parts to work right, I, after 8 years of being vegan, decided that I needed more animal protein and went back to eating animals, mainly fish and chicken. During this period of time I noticed that my urine had a peculiar strong smell that lingered in the bathroom, but then I noticed that on days when I had little protein, the smell was gone. This led me to Hull's book. I am back on a low protein vegan diet and my eGFR has gone from a low of 30 in the hospital to 57. In recent blood tests, the eGFR went from 44 to 57 in two weeks after the diet change. When I told the nephrologist what I was doing, she was all for it and very supportive, but I suspect that if she stepped out of the approved narrative for the treatment of kidney disease in the medical community, she'd be facing rough criticism.",
             "Paperback"],
            ["R1ZWA5FR9RIPUE","2020-10-13",5,True,29,"Such a Godsend!",
             "I had a severe kidney infection when I was a kid (blood in urine, etc) but recovered. Ever since then, I've felt my kidneys were delicate but no doctor ever mentioned anything. I'm always told my kidney values are normal and nothing more. Last week I got a message from my doctor that my kidney labs were a little low and that I needed to do more tests, including an ultrasound. AND, that it was urgent that I get tested immediately! No other information was given and of course, I couldn't actually talk to the doctor to get more details. I was in a panic! I didn't know if my numbers were just a tiny bit off or if I was headed for dialysis. Because too many doctors don't tell you ANYTHING. I cannot say how much comfort this book has given me over this long week of stress and worry. I know that even if I have kidney disease, there is something I can DO ABOUT IT. My GFR is 52, which would be Stage 3a kidney disease. I have confidence that if I continue to follow the advice in this book that my GFR will improve.",
             ""],
            ["R1UZO43Q5U3PKU","2019-09-23",5,True,157,"Challenging to read, but a real eye-opener!",
             "I have been a type 2 diabetic for 25 years now. Two months ago, my GP in Costa Rica tells me that I have kidney disease. I was STUNNED! I decided to try a strict Vegan diet, with emphasis on the PRAL, alkaline food values. My eGFR has now IMPROVED from 42% to 52% in two months - I am now only stage 3a. In my opinion Lee has done an amazing amount of valuable research. The program seems to WORK!!",
             "Kindle"],
            ["R1W023Y98YW5VR","2019-08-30",5,True,145,"You Probably Need This Book",
             "Kidney disease is on the rise, and is a silent killer. Imagine my surprise to learn through blood and urine tests that I already have advanced kidney disease. My eGFR is only 57 - Stage 3 kidney disease. This book is written in easy to understand language; the studies are summarized. This book lets us know that kidney disease is not curable and is progressive, but we can lower the demands we make on our kidneys and thus help them function better, putting us into remission. A strict low-protein diet is recommended along with a proprietary supplement developed by the author.",
             "Paperback"],
            ["R223WMO2TKM9JD","2019-02-06",5,True,398,
             "You can save your kidneys if you're serious. Read this book!!!",
             "Mr. Hull is brilliant and has put mind-breaking effort into identifying the variables that affect kidneys, researching hundreds and hundreds of abstracts. Diet affects kidney disease, something the medical world routinely dismisses. This book is groundbreaking inasmuch as there is NO OTHER RESOURCE like it. Over a two month period my GFR had descended from 41 to 38, and at that rate I would be looking at dialysis in about 7 months. I was frantic. I found Stopping Kidney Disease online, read each chapter carefully and extrapolated the information into an aggressive diet of no meat, no fish, no eggs, no dairy and no grains. In two months, my GFR soared from 38 to 56 and my doctors jaws literally dropped.",
             "Paperback"],
            ["R326X60IDH38EY","2019-03-23",4,True,133,
             "Preserve Kidney Function Now! A Guide on How to Stop the Downward Spiral of CKD",
             "If you have been referred to a nephrologist you are probably a little worried about your health. After my first appointment with my new doctor I realized that I was not going to get a lot of detailed information about managing Chronic Kidney Disease (CKD). Lee Hull also seems to be looking out for the reader by giving you all the facts you should be considering. My GFR is at 52 currently and so I'm in stage 3. P.S. - 12/11/2023 - I started taking acacia fiber and that made my GFR go back up to 68. I am not a doctor, nor is this medical advice. I should technically be dead by now because my GFR was in the 40s.",
             "Kindle"],
            ["RK1IXVH8E107L","2019-07-16",3,True,44,
             "Very well researched, but short on practical application",
             "I was recently diagnosed with stage 3 CKD with GFR 32. Lee's book is a wonderful reference about what is happening in our bodies when we have CKD - very well researched and documented. And, as the other reviews have said, is VERY technical and somewhat difficult to understand. My SKEPTICISM about the book is that Lee is selling a product, Albutrix (a nitrogen free albumin supplement) in his prescription for healing your kidneys. Also, there is virtually no information about Lee's background - is he a scientist, doctor, nutritionist? What are his credentials? The third reason I give this book a 3 is that even though Lee gives excellent scientific information, there is very little practical application.",
             "Paperback"],
        ]
    },
    {
        "sheet": "Food Guide",
        "asin": "0578493624",
        "avg_rating": 4.3,
        "total_ratings": 2635,
        "reviews_scraped": 8,
        "dataset_id": "DfKJguPyRWXRSzFT0",
        "rows": [
            ["R16ISIRVLMR84G","2026-01-02",5,True,9,"This cookbook has changed my life",
             "This is an excellent cookbook for CKD patients on low protein diets. It is meant that you read the book Stopping Kidney Disease first, but there is enough basic information in this cookbook to get you started. My bloodwork numbers really balanced out after being on this diet. My blood albumin went from low into mid-normal range. My BUN went to normal. Many other numbers normalized. My energy is way up, and I feel a lot better. The recipes are good, and you can sign up for emails for more recipes. Highly recommend if you are a kidney patient.",
             "Paperback"],
            ["R2V9VVFB2ZPZXU","2025-07-24",5,True,29,"This book saved my life.",
             "On 10/03/24 my eGFR was 22. I was totally shocked when the doctor told me that if it went much lower I would need dialysis. His only suggestion was to drink more water and stop taking NSAIDS. I started scouring the internet for advice and found Lee's book. I started following the eating plan on 10/07/24. By 10/30/24 my eGFR was 39. By Christmas my GFR was 63. I stopped ALL red meat, pork, chicken, and all seafood, except for salmon. I only eat fresh food that I cook myself. In the process I have lost 85 lbs. After seeing my results, my doctor has recommended this book to more than 100 of his patients.",
             "Paperback"],
            ["RMJ7J2Z9MKAO2","2025-05-22",5,True,15,
             "Good Start for a Kidney Friendly Diet BUT Needs Editing for Errors",
             "The recipes in the book are good. Having said that, be prepared to eat lots of salads. Prices in the book are very understated. Also, there are a lot of errors in the recipes. Ingredients that are mentioned in the preparation are missing in the ingredient list and in one case, the ingredients for an important sauce are missing. I'm an experienced cook so these errors didn't bother me but some people might be confused.",
             "Paperback"],
            ["R1FCBPHS78HV2G","2024-03-13",5,True,65,
             "Delicious Easy Recipes with Great Flavor and Healthy Ingredients!",
             "This cookbook has delicious recipes that are easy to make. If you are looking to increase fruits and vegetables in your meals this is a great cookbook. Each recipe has a nice photograph plus cost analysis and nutrition facts. I don't see any sugar being used in these recipes, just calorie free sweeteners of your choice. The recipes are also low-salt to no salt. So far the recipes I've tried have been great. P.S. Update 6/25/2024 - So eating more vegetables and fruit really does work. My GFR is back up to 61 after it had fallen to 58.",
             "Paperback"],
            ["R2VIIW7D8V7BBC","2022-06-01",5,True,156,"REVERSED KIDNEY DECLINE IN 30 DAYS",
             "Stopping Kidney Disease Diet Book literally has saved my only kidney! Just three months ago I learned that my only kidney had lost 50% of its function in just one year. My eGFR was slipping downward way too fast and was in the low 30s when a random blood test caught the issue. At 73 years of age with multiple chronic conditions, I realized that if I didn't make some big changes, I would be on dialysis within a few months. I found the Stopping Kidney Disease books, followed the diet and within 30 Days I completely reversed the declining eGFR back up to 74.",
             "Paperback"],
            ["RBLSFWCUUA1ZX","2020-11-20",5,True,119,
             "Excellent - don't be put off by the near-vegan recipes, they are necessary for kidney healing",
             "Yes, some people will be immediately put off by the diet, as it is almost vegan. But the author, Mr Hull, explains WHY this is - our weakened kidneys have to struggle when dealing with the high acid load in meat and a lot of dairy products. The diet is not vegan, vegetarian, or any ian diet; it pushes no political, philosophical, or any other agenda, other than let's eat better for our kidneys. Meat, cheese, and dairy are hard on our kidneys. Complaining about this fact doesn't change anything.",
             "Paperback"],
            ["RDD0AE0Q0JQ5K","2025-02-07",4,True,2,"Adjustment. Healthy and tasty",
             "I am a foodie. I am a cook. I love ethnic recipes. This covers many areas and when I feel tired of trying to make something easy, Healthy and enjoyable I snatch it up. I am finding more Japanese cooking to be more kidney friendly unlike Chinese and Korean. I love that I don't have to research the nutritional information myself. However, the potassium levels were inconsistent across recipes.",
             "Kindle"],
            ["RBX6751FD7ZLZ","2026-01-14",3,True,4,"Use with discretion before making a commitment.",
             "If you are thinking of trying this diet, be careful, really check with your doctor. There might be some restrictions for you, there was for me. I am a diabetic and this food regime is too high in carbs, etc. Please check first about your own need and restrictions. Otherwise, as far as the book itself, it is beautifully written and arranged and for the right person could be a good guide to better health and better kidney function.",
             "Spiral-bound"],
        ]
    },
    {
        "sheet": "Basics",
        "asin": "1734262419",
        "avg_rating": 4.3,
        "total_ratings": 450,
        "reviews_scraped": 8,
        "dataset_id": "CpF3EgBraBteTaW0c",
        "rows": [
            ["RBJ8CG24ENSWS","2025-08-19",5,True,1,
             "It was a simple book that explained kidney disease.",
             "It was a good book that explains information about kidney disease and if you read it, follow a kidney friendly diet and also read some of the useful information about kidney disease on the internet, it will help you live with kidney disease.",
             "Paperback"],
            ["R3DR30FNCR4L70","2024-02-23",5,True,37,
             "Start Reading and Start Healing! How to Get Your GFR higher with Early Intervention!",
             "As someone who started my Chronic Kidney Disease journey in 2015 with a GFR of 57, which is stage 3, I have been all the way down to a GFR of 42 and recently as high as 68. Currently I am at a GFR of 58 and I'm doing fairly well. After reading this book I realize I've defied the odds and am still alive nearly ten years later. This fairly new book from 2022 has lots of patient success stories! Lee Hull always compiles very organized and useful books. He also provides the most up-to-date information. I believe Acacia fiber has helped me the most - I take one tablespoon in my coffee each morning. If my GFR can go from 42 to 68 because I started doing that and taking a kidney supplement, then I think there is hope.",
             "Paperback"],
            ["R8W60GGDDZ6TM","2023-08-18",5,True,10,"Great book! Just what you need!",
             "Great book if you or a loved one needs to pay diligent attention to kidney health and optimize function outside of doctors. Doctors want to prescribe drugs not get into the weeds with what you can and should do on your own. Good doctors should do both! This book is fairly priced with critical info and no filler just to hit page counts.",
             "Paperback"],
            ["R2X35O3KIB4OSU","2023-03-21",5,True,15,
             "Stop kidney challenges by changing your diet",
             "This is the first book that the information is easy to follow. As I read the book I could not put it down. Plus I began to implement the many suggestions. He talked about the trifecta. It is so true. There are three factors I must look at. I recommend this book for anyone with kidney disease.",
             "Paperback"],
            ["R6GR56YDZ7P13","2022-06-20",5,True,27,
             "Nice, condensed, informative edition! Highly recommend!",
             "I already own a full copy of the full edition of this book. This is also a great book, but it is condensed down in laymen's easy to understand wording and smaller in size. As a person with CKD I can only say that this book is an invaluable resource to share with your family, friends and physicians. Not as thorough as the full version with tests, etc. but complete. Highly recommend.",
             "Paperback"],
            ["R1PBN9YDBW2A91","2022-03-26",5,True,13,"Very understandable",
             "I ordered the first Stopping Kidney Disease Book and was really overwhelmed with trying to retain all the information plus know how to incorporate it into my life. I thought many times that I wish there was a condensed or simplified how-to version. I must have not been the only one to feel that way because this book was written. Oh so much easier to retain the information because it is very understandable and makes so much sense.",
             "Paperback"],
            ["R6X7EVMS64B7I","2025-03-07",3,True,11,"Book is mostly promoting two supplements.",
             "This book has some good information, but most of the book promotes two supplements to take for Kidney disease: Albutrix and Microtrix. These supplements are very expensive. Personally, I don't think enough research or clinical trials have been done on these.",
             "Paperback"],
            ["R22BICM5J4CYIK","2022-07-20",3,True,68,"Not for everyone",
             "I was (mistakenly) diagnosed with stage 3 Chronic Kidney Disease or CKD3. I panicked and looked for solutions to slowing or reversing the disease. I came across this book and its companion book, Stopping Kidney Disease Food Guide. After a week of unsatisfying meals, I sat down with my daughter to discuss my options. With the genes I was blessed with, at age 73, I have a good 10-12 years ahead of me. We agreed to modifying my eating habits considerably, but not to deprive myself so much that I would not look forward to those remaining years. I made changes in my salt and protein consumption and cut out high potassium foods. This eating plan is great for people who can stick to it.",
             "Paperback"],
        ]
    },
    {
        "sheet": "Kidney Failure & Transplant",
        "asin": "B082S6ZRVB",
        "avg_rating": 4.4,
        "total_ratings": 39,
        "reviews_scraped": 8,
        "dataset_id": "o2zw1XVQR0iFOouzd",
        "rows": [
            ["R1S84E1RCKF1YI","2022-01-25",5,True,2,"Extremely helpful resource",
             "I ordered this book for my father who is experiencing kidney failure, dialysis is looming for him, and he is trying to navigate getting on a transplant waitlist. It has been a very helpful and reassuring guide for him, written by two highly-knowledgeable and respected physicians at the Wisconsin Department of Medicine. It is much more convenient and trustworthy to have a guide to refer to that has been written by physicians who work with patients with kidney failure everyday, and who are at the forefront of research in their field.",
             "Paperback"],
            ["R34VR9NOO5ALO9","2021-01-01",5,True,2,"Informative, short and easy read",
             "This book is a superb simple but very informative book and explains the non technical people the options available for kidney failures. If you are looking for a book for either your family, relatives or friends, who are going through kidney issues or you would like to learn about it, please look into this. Drs Aziz and Parajuli have written this book for people with basic knowledge about kidney failures and treatments available.",
             "Paperback"],
            ["R2DQTXQVHLBEJ7","2020-06-07",5,True,1,"Short read and informative",
             "A short read but quite informative on the subject matter. The doctors/authors kept the audience in mind - written in layman's language so a person with non-medical background can read and easily digest the information. The book does a good job in preparing the patient party with a list of questions for their doctor's visit. Definitely one of the books to be considered/included by their doctors.",
             ""],
            ["R1FU0X6Q7MRYOW","2020-05-22",5,True,1,"Easy to understand",
             "Took awhile to receive, but got it","Paperback"],
            ["RU18TVA7ODMT","2020-02-20",5,True,2,"Excellent resource - highly recommend",
             "What a great resource for patients and families going through this process! I work with patients with renal failure undergoing evaluation for kidney transplant, and I will be recommending this book to all of them.",
             "Paperback"],
            ["R1RQR84I6L1854","2020-02-04",5,True,0,"A very well written book!!!",
             "With its easy to understand language, this book is a go to book for the patients and their families for a simpler perspective and understanding of the complex issues of kidney diseases. For the medical students and professionals this book can become quite handy for a quick reference. Great work Dr. Parajuli and Dr. Aziz!",
             ""],
            ["R3KJ8JAD28HAW8","2020-01-04",5,True,0,"Very well written book!",
             "Well written facts in easy language for patients. As a primary care physician I totally see recommending it to my patients on the path of transplant.",
             "Paperback"],
            ["R38UMPIXOWKV89","2020-01-02",5,True,5,"Very helpful",
             "The scariest part of dealing with chronic disease is often feeling that you simply do not comprehend everything that is happening with your body. This is written by doctors FOR lay people. It avoids doctor-speak, and finds ways to explain terminology that anyone can understand, with the goal of helping individuals take a more active role in their own health decisions. Probably the most uplifting part of the book for me is the passage written by a transplant recipient.",
             ""],
        ]
    },
]

# ── 1. Create spreadsheet ───────────────────────────────────────────────────
print("Creating spreadsheet...")
resp = gws("sheets", "spreadsheets", "create", body={
    "properties": {"title": "KidneyHood Amazon Reviews"},
    "sheets": [
        {"properties": {"title": "Summary",                      "index": 0}},
        {"properties": {"title": "Evidence-Based Guide",         "index": 1}},
        {"properties": {"title": "Stopping Kidney Disease",      "index": 2}},
        {"properties": {"title": "Food Guide",                   "index": 3}},
        {"properties": {"title": "Basics",                       "index": 4}},
        {"properties": {"title": "Kidney Failure & Transplant",  "index": 5}},
    ]
})
sid = resp["spreadsheetId"]
url = resp["spreadsheetUrl"]
print(f"  Spreadsheet ID: {sid}")
print(f"  URL: {url}")

# ── 2. Build batchUpdate payload ────────────────────────────────────────────
data_ranges = []

# Summary sheet
summary_rows = [
    ["KidneyHood Amazon Reviews — Scrape Summary"],
    ["Generated", "2026-04-13"],
    [],
    ["Book", "ASIN", "Avg Rating", "Total Ratings", "Reviews Scraped", "Apify Dataset ID"],
]
for b in BOOKS:
    summary_rows.append([
        b["sheet"], b["asin"], b["avg_rating"],
        b["total_ratings"], b["reviews_scraped"], b["dataset_id"]
    ])
summary_rows += [
    [],
    ["Rating Distribution"],
    ["Book", "5-star %", "4-star %", "3-star %", "2-star %", "1-star %"],
    ["Evidence-Based Guide",  "73%","13%","10%","3%","3%"],
    ["Stopping Kidney Disease","67%","16%","9%","3%","5%"],
    ["Food Guide",            "66%","16%","10%","4%","4%"],
    ["Basics",                "63%","17%","10%","5%","5%"],
    ["Kidney Failure & Transplant","68%","18%","10%","0%","4%"],
]
data_ranges.append({"range": "Summary!A1", "values": summary_rows})

# Per-book sheets
for b in BOOKS:
    rows = [HEADERS]
    for r in b["rows"]:
        rows.append([
            r[0], r[1], r[2],
            "Yes" if r[3] else "No",
            r[4],
            clean(r[5]),
            clean(r[6]),
            r[7],
        ])
    data_ranges.append({"range": f"'{b['sheet']}'!A1", "values": rows})

# ── 3. Write all data in one batchUpdate ────────────────────────────────────
print("Writing data to all sheets...")
gws("sheets", "spreadsheets", "values", "batchUpdate",
    params={"spreadsheetId": sid},
    body={"valueInputOption": "RAW", "data": data_ranges})

print("\nDone!")
print(f"Spreadsheet URL: {url}")
