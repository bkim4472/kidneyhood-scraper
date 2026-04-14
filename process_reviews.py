#!/usr/bin/env python3
"""
KidneyHood Amazon Reviews - Full Processing Script
Fetches data from saved Apify dataset files, deduplicates, normalizes, and exports to CSV.
"""

import json
import csv
import re
import os
from datetime import datetime
from collections import defaultdict

TOOL_DIR = "/Users/brandkim/.claude/projects/-Users-brandkim---kidneyhood-scraper/3213c424-4b46-477f-a974-9e455c5d433c/tool-results"

ASIN_TO_BOOK = {
    "B0DM96NVSX": "Evidence-Based Guide to Kidney and Renal Diets",
    "0692901159": "Stopping Kidney Disease",
    "0578493624": "Stopping Kidney Disease Food Guide",
    "1734262419": "Stopping Kidney Disease Basics",
    "B082S6ZRVB": "Kidney Failure & Transplantation",
}

ASIN_TARGETS = {
    "B0DM96NVSX": 21,
    "0692901159": 220,
    "0578493624": 230,
    "1734262419": 49,
    "B082S6ZRVB": 16,
}

# ── Inline data captured from API responses ──────────────────────────────────

# Dataset 4PffBeuoEsod3CZrY  (Evidence-Based, junglee, 21 items)
DS_4PffBeuoEsod3CZrY = [{"reviewTitle":"Delivery as requested.,","reviewDescription":"Excellent information, great research..","ratingScore":5,"reviewId":"R16HS1WDS7OZKG","date":"2026-02-23","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Vee jo","productAsin":"B0DM96NVSX"},{"reviewTitle":"Get all of his books. His concepts work if you work them.","reviewDescription":"If you have CKD Lee Hull's books will be a life changer IF you follow his advice. eFGR 52 May 2024. Big surprise to me I'd been 62-70 for the past 4 years my doc never mentioned a thing. I was already in stage 2. In the fall I started seeing a kidney dietician and went on a more plant based diet, she was nice but really not that much help. I saw a nephrologist in December who put me on Farizga (very expensive). I then found Lee's books. Changed my eating plan completely and went from 59 in October to 64 in Feb to 71 in June 6 2025. Stopped the Farziga in Feb since I was getting side effects and did not need it. YES you do need dramatic changes and as someone who also has CKD said to me I don't like vegetables and don't care if I live longer. Well I'm 74 and i want to live till my 90''s and see my Grandkids grow up. If no meat and a very low protein diet keeps me off dialysis and healthy it's worth it to me. Like most things it's always up to the individual to make and follow his decisions.","ratingScore":5,"reviewId":"R1MTTP1PPWVKRC","date":"2025-06-16","reviewReaction":"11","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Vchip","productAsin":"B0DM96NVSX"},{"reviewTitle":"Easy to understand and a great guide","reviewDescription":"I had researched the topic, and the author was very thorough. Easy to read, but best of all the information tracked what my nephrologist recommended already. The information did improve my lab results and will give you the confidence to make better dietary decisions. That goes a long way in staying on a strict kidney friendly diet. My eGFR was dramatically improved over a 6 month period. As always, check with your doctors before making any changes in your treatment plan.","ratingScore":5,"reviewId":"R41FES3HKBUOK","date":"2025-08-31","reviewReaction":"6","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"J. Cranford","productAsin":"B0DM96NVSX"},{"reviewTitle":"Read Stopping Kidney Disease Instead","reviewDescription":"After reading Lee Hull's longer book, Stopping Kidney Disease, I changed to a very low protein vegan diet and used Albutrix tablets for most of the last 3 years. The Evidence-Based Guide to Kidney and Renal Diets is a shorter book that added nothing new to what I knew. The new feature is a collection of selected case studies. Hull did not analyze the results of all his Albutrix users or take a random sample of his users. He excluded some users, including me, from his study. Readers should note that there is no known way of stopping the progression of chronic kidney disease (CKD). Many nephrologists promote very dangerous drugs to slow the progression because they know that many patients will refuse to change their diets. These drugs have side effects such as death, gangrene, and serious yeast infections. After switching to a vegan diet, there were some early gains: (1) A1C fell from 6.1 to less than 5; no longer pre-diabetic (2) BUN dropped from 26 to as low as 8. BUN decreases because Albutrix tablets contain the lowest nitrogen content of any protein supplement. While some protein supplements disclose amino acid composition, I have not found any that will disclose nitrogen content. Albutrix discloses nitrogen content but does not disclose amino acid composition. A problem with a vegan diet is that my red blood cell numbers went from normal to bad. Having had a calcium oxalate kidney stone, I needed to switch also to a low oxalate diet. That makes it very difficult to consume enough calories daily. The result is protein energy wasting. If your body cannot find enough energy from your diet, your body will burn protein for energy. If I cannot solve the red blood cell problem, then I shall need to switch to a low protein diet and to stop using Albutrix. Instead of the Evidence book, read the Stopping book and research also newer findings.","ratingScore":3,"reviewId":"R37PRJIZBRN6M9","date":"2025-01-12","reviewReaction":"23","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Paul Sheldon Foote","productAsin":"B0DM96NVSX"},{"reviewTitle":"Great resource","reviewDescription":"There's so much information in this book everyone with CKD should buy this book.","ratingScore":5,"reviewId":"R3MVBKEP41T3TF","date":"2026-01-06","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Troy Shoemaker","productAsin":"B0DM96NVSX"},{"reviewTitle":"He has more books of interest","reviewDescription":"Very informative, I needed that.","ratingScore":5,"reviewId":"R1G1T482FD26I7","date":"2025-12-19","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"nancy carrillo","productAsin":"B0DM96NVSX"},{"reviewTitle":"Best Kidney advice","reviewDescription":"This book is good for getting you to a point where you can make a sound decision to get and keep your diet clean and how to do it the right way.","ratingScore":5,"reviewId":"RVWEF8FKA7D6O","date":"2025-02-28","reviewReaction":"6","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Diahla","productAsin":"B0DM96NVSX"},{"reviewTitle":"Excellent factual guide to KCD.","reviewDescription":"One of the only true factual guides to chronic Kidney disease Information","ratingScore":5,"reviewId":"R27R5B4CA7XXK3","date":"2025-08-16","reviewReaction":"6","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"bhiker","productAsin":"B0DM96NVSX"},{"reviewTitle":"Lee Hull is a very smart guy.","reviewDescription":"Lee Hull is smart and really trying to help people. He has been through the process of treating his own ckd as he describes in his books","ratingScore":5,"reviewId":"R3CD9XJWKXKB9K","date":"2025-09-07","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Peter Rogers MD","productAsin":"B0DM96NVSX"},{"reviewTitle":"Very very strict diet.","reviewDescription":"Everything you need to know about a diet for kidney disease.","ratingScore":5,"reviewId":"RYU4U4CWYQVPH","date":"2025-07-20","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"marggy","productAsin":"B0DM96NVSX"},{"reviewTitle":"Helpful book","reviewDescription":"If you have to have CKD, it's good to have a few resources to make sure you understand your numbers, your symptoms and all about this disease. This is a very comprehensive book.","ratingScore":5,"reviewId":"R9SNT9R6UO14H","date":"2025-04-11","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Snowbound","productAsin":"B0DM96NVSX"},{"reviewTitle":"Best info","reviewDescription":"This book has best info for ckd patients . The most comprehensive studies done and advise how to handle ckd and improve your numbers .. highly recommend if you or loved one has stage 3 or 4 ckd","ratingScore":5,"reviewId":"RDER8BQEXICXA","date":"2024-12-10","reviewReaction":"8","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Rhoda Galvani","productAsin":"B0DM96NVSX"},{"reviewTitle":"Evidence based Kindney and Renal Diet","reviewDescription":"Purchased the newest edition for reference and review having shared my previous edition with a friend in need. Would recommend.","ratingScore":5,"reviewId":"R3IAVHQZD56LKH","date":"2025-02-17","reviewReaction":"4","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"YazSox","productAsin":"B0DM96NVSX"},{"reviewTitle":"Very helpful book","reviewDescription":"Good information, very helpful, easy to understand. Has simple recipes.","ratingScore":5,"reviewId":"R2K6P4Q3X1XFG0","date":"2025-03-06","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Wonderful cream","productAsin":"B0DM96NVSX"},{"reviewTitle":"Very good.","reviewDescription":"Love all the information but am waiting for more delicious recipes! Thank you!!","ratingScore":5,"reviewId":"R35UIMBGWSYVN5","date":"2024-12-28","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Amazon Customer","productAsin":"B0DM96NVSX"},{"reviewTitle":"The most current information for taking charge of your kidney health.","reviewDescription":"I have been reading and following Lee Hull since his first book in 2019. I have been able to maintain my kidney status since 2019 by following his advice and recommended diet. It is important to be your own advocate in all health matters - and the information provided in Lee Hull's books gives you the guidance you need to be confident that what you are doing is right for you. I highly recommend this book and all of Lee Hull's books if you or one of your family members has Chronic Kidney Disease.","ratingScore":5,"reviewId":"RS7F2K7LLVZVS","date":"2024-11-20","reviewReaction":"13","isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":"SunnySD107","productAsin":"B0DM96NVSX"},{"reviewTitle":"Very informative.","reviewDescription":"A great resource for anyone looking to better understand kidney health and take steps toward prevention.","ratingScore":5,"reviewId":"R3OAJU3ZETYADN","date":"2025-01-14","reviewReaction":"4","isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":"Amazon Customer","productAsin":"B0DM96NVSX"},{"reviewTitle":"Surprise, surprise","reviewDescription":"Finally an excellent book . I have been dealing with CKD for over 10 years and the book answered many questions and created a few more for my Doctor. Many thanks.","ratingScore":5,"reviewId":"R11EEFF9XFFWXT","date":"2024-11-17","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":"Amazon Customer","productAsin":"B0DM96NVSX"}]

# Dataset KUBiW1pBhtYvS2u0y (Evidence-Based, junglee, adds 1-star and 2-star)
DS_KUBiW1pBhtYvS2u0y = [{"reviewTitle":"Doctors Were The Most Successful Out Of Anyone Who Use These Specific Tools","reviewDescription":"That along was enough for me to pass on and not look any further. Because if this group who has the ability to have an advantage that most people, what's the point to your research for others unlike yourselves.","ratingScore":1,"reviewId":"RM5DWWNYKOS5Q","date":"2026-02-19","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"B0DM96NVSX"},{"reviewTitle":"Understanding","reviewDescription":"Wasn't quite as easy to understand","ratingScore":2,"reviewId":"R2NY3JZVL7T4YU","date":"2025-09-05","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B0DM96NVSX"},{"reviewTitle":"Not useful","reviewDescription":"I ordered this because it was recommended reading by kidney organization. If you are looking for a reference regarding various kidney studies and statistics about preserving kidney function then this book is for you. I think there are better sources out there written by nephrologist.","ratingScore":2,"reviewId":"R1N5BHIA3OBX4K","date":"2024-11-25","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B0DM96NVSX"}]

# keyword runs for Food Guide
DS_ZRtyz4Sxp4S4zrEJ5 = [{"reviewId":"R1FCBPHS78HV2G","reviewTitle":"Delicious Easy Recipes with Great Flavor and Healthy Ingredients!","reviewDescription":"This cookbook has delicious recipes that are easy to make...","ratingScore":5,"date":"2024-03-13","reviewReaction":"65","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R3N90G5970COC","reviewTitle":"good reference","reviewDescription":"I shouldn't have to explain about the valuable information contained in this book...","ratingScore":5,"date":"2024-09-14","reviewReaction":"5","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RNRF9JQIF4E42","reviewTitle":"Saved My Mother's Life","reviewDescription":"My mom's nephrologist told me that my putting my mother on this diet for the past 3 and half years has saved her life...","ratingScore":5,"date":"2023-07-29","reviewReaction":"41","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R27AMQC3HJNKJB","reviewTitle":"Guide for stopping kidney disease","reviewDescription":"This book is loaded with information and tips on how to become healthy with real food...","ratingScore":5,"date":"2024-11-02","reviewReaction":"6","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"RIZSF2USVQECG","reviewTitle":"Great recipe book!!","reviewDescription":"I have followed several of these recipes...they are tasty and healthy and also filling...","ratingScore":5,"date":"2024-07-22","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RHYP33OVCRXCS","reviewTitle":"Amazing!!!","reviewDescription":"Best book for learning how to eat the right foods with recipes for kidney help...","ratingScore":5,"date":"2024-02-01","reviewReaction":"6","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R326DPQ2T5OLJ6","reviewTitle":"Needed this so I can cook for my Guy and keep him healthy","reviewDescription":"Perfect guide for cooking healthy.","ratingScore":5,"date":"2024-09-12","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2MKWPPUCQQWYL","reviewTitle":"Very Informative","reviewDescription":"Everything you need to know about your kidney!...","ratingScore":5,"date":"2025-07-18","reviewReaction":"1","isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RPSBL8Q0GBGXH","reviewTitle":"Good Food Guide","reviewDescription":"To cut down on shipping costs, I purchased the two books...","ratingScore":5,"date":"2024-06-07","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R16ISIRVLMR84G","reviewTitle":"This cookbook has changed my life","reviewDescription":"This is an excellent cookbook for CKD patients on low protein diets...","ratingScore":5,"date":"2026-01-02","reviewReaction":"9","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RDD0AE0Q0JQ5K","reviewTitle":"Adjustment. Healthy and tasty","reviewDescription":"I am a foodie...","ratingScore":4,"date":"2025-02-07","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"RBLSFWCUUA1ZX","reviewTitle":"Excellent—don't be put off by the near-vegan recipes, they are necessary for kidney healing","reviewDescription":"Yes, some people will be immediately put off by the diet, as it is almost vegan...","ratingScore":5,"date":"2020-11-20","reviewReaction":"119","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RBX6751FD7ZLZ","reviewTitle":"Use with discretion before making a commitment.","reviewDescription":"If you are thinking of trying this diet, be careful, really check with your doctor...","ratingScore":3,"date":"2026-01-14","reviewReaction":"4","isVerified":True,"isAmazonVine":False,"variant":"Format: Spiral-bound","username":None,"productAsin":"0578493624"},{"reviewId":"R2VIIW7D8V7BBC","reviewTitle":"REVERSED KIDNEY DECLINE IN 30 DAYS","reviewDescription":"Stopping Kidney Disease Diet Book literally has saved my only kidney!...","ratingScore":5,"date":"2022-06-01","reviewReaction":"156","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RMJ7J2Z9MKAO2","reviewTitle":"Good Start for a Kidney Friendly Diet BUT Needs Editing for Errors","reviewDescription":"The recipes in the book are good...","ratingScore":5,"date":"2025-05-22","reviewReaction":"15","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2V9VVFB2ZPZXU","reviewTitle":"This book saved my life.","reviewDescription":"On 10/03/24 my eGFR was 22...","ratingScore":5,"date":"2025-07-24","reviewReaction":"29","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2NZOKFE0R5MWQ","reviewTitle":"Need more recipes","reviewDescription":"Too complicated I just wanted to book with recipes...","ratingScore":3,"date":"2026-03-12","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R1F13DZWX3QYY4","reviewTitle":"Not useful for the later stages of kidney disease","reviewDescription":"This Stopping Kidney Disease Food Guide is way too high in sodium, phosphorus, and potassium...","ratingScore":3,"date":"2026-01-27","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"}]

DS_5L9W8RcCky1diBaMa = [{"reviewId":"R16ISIRVLMR84G","reviewTitle":"This cookbook has changed my life","reviewDescription":"","ratingScore":5,"date":"2026-01-02","reviewReaction":"9","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2V9VVFB2ZPZXU","reviewTitle":"This book saved my life.","reviewDescription":"","ratingScore":5,"date":"2025-07-24","reviewReaction":"29","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RYB8W123U6GGR","reviewTitle":"Lee Hull is an inspiration, as one who beat kidney disease using strict diet.","reviewDescription":"I am trying to follow the diet recommendations in this informative cookbook as a stage 3 patient...","ratingScore":5,"date":"2019-07-06","reviewReaction":"90","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R28RMW13NNNK99","reviewTitle":"Diet is the key","reviewDescription":"After reading this book, I now know the best things to eat for breakfast, lunch and dinner...","ratingScore":5,"date":"2023-03-21","reviewReaction":"9","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R1XPRC6F8XFACV","reviewTitle":"Good information","reviewDescription":"Excellent book , author writes knowledgeable information. I would recommend this book","ratingScore":5,"date":"2024-01-29","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2H2VDTBSAH3QB","reviewTitle":"Book : Stopping Kidney Disease Food Guide","reviewDescription":"If U have CKD .. Look no further. Highly recommend this Food Guide...","ratingScore":5,"date":"2021-05-21","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"0578493624"},{"reviewId":"R16NR192Y4BC5S","reviewTitle":"Read both books if you have Chronic Kidney Disease!","reviewDescription":"Great companion book to the Stopping Kidney Disease book...","ratingScore":5,"date":"2019-07-19","reviewReaction":"56","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RHXG146WJZZJ8","reviewTitle":"Read everything from Lee Hull","reviewDescription":"This is without question the best and most effective program to improve kidney function...","ratingScore":5,"date":"2020-12-10","reviewReaction":"1","isVerified":False,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"0578493624"},{"reviewId":"R31QZLCU8NJW2Z","reviewTitle":"Excellant!","reviewDescription":"Very informative with tasty recipies! Highly recommend!","ratingScore":5,"date":"2021-01-09","reviewReaction":"1","isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RBLSFWCUUA1ZX","reviewTitle":"Excellent—don't be put off by the near-vegan recipes","reviewDescription":"","ratingScore":5,"date":"2020-11-20","reviewReaction":"119","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RDD0AE0Q0JQ5K","reviewTitle":"Adjustment. Healthy and tasty","reviewDescription":"","ratingScore":4,"date":"2025-02-07","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"RBX6751FD7ZLZ","reviewTitle":"Use with discretion before making a commitment.","reviewDescription":"","ratingScore":3,"date":"2026-01-14","reviewReaction":"4","isVerified":True,"isAmazonVine":False,"variant":"Format: Spiral-bound","username":None,"productAsin":"0578493624"},{"reviewId":"R2VIIW7D8V7BBC","reviewTitle":"REVERSED KIDNEY DECLINE IN 30 DAYS","reviewDescription":"","ratingScore":5,"date":"2022-06-01","reviewReaction":"156","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RMJ7J2Z9MKAO2","reviewTitle":"Good Start for a Kidney Friendly Diet BUT Needs Editing for Errors","reviewDescription":"","ratingScore":5,"date":"2025-05-22","reviewReaction":"15","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R1WWWQMUF7IQJ","reviewTitle":"The only recipe book you need for people with CKD","reviewDescription":"I rarely write reviews, but for this one, I'm not gonna pass it up...","ratingScore":5,"date":"2019-07-20","reviewReaction":"63","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"R2YHCRD3OJ0Q2U","reviewTitle":"Most helpful food guide for GFR/Kidney diet","reviewDescription":"Lee Hull's first book was immensely helpful...","ratingScore":5,"date":"2019-06-30","reviewReaction":"43","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"R26OPN0FBVKK9P","reviewTitle":"Yikes","reviewDescription":"This book is very informative and scary and depressing...","ratingScore":5,"date":"2022-02-06","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R2X1PK8DA1CXMQ","reviewTitle":"Best kidney disease books","reviewDescription":"26 year old son was diagnosed with renal failure...","ratingScore":5,"date":"2022-10-19","reviewReaction":"5","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"R1FCBPHS78HV2G","reviewTitle":"Delicious Easy Recipes with Great Flavor and Healthy Ingredients!","reviewDescription":"","ratingScore":5,"date":"2024-03-13","reviewReaction":"65","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"0578493624"},{"reviewId":"RDD0AE0Q0JQ5K","reviewTitle":"Adjustment. Healthy and tasty","reviewDescription":"","ratingScore":4,"date":"2025-02-07","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"0578493624"},{"reviewId":"RBX6751FD7ZLZ","reviewTitle":"Use with discretion before making a commitment.","reviewDescription":"","ratingScore":3,"date":"2026-01-14","reviewReaction":"4","isVerified":True,"isAmazonVine":False,"variant":"Format: Spiral-bound","username":None,"productAsin":"0578493624"}]

# Basics OTaP0rqmtmca4or9B (inline)
DS_OTaP0rqmtmca4or9B = [{"reviewTitle":"Start Reading and Start Healing! How to Get Your GFR higher with Early Intervention!","reviewDescription":"As someone who started my Chronic Kidney Disease journey in 2015 with a GFR of 57, which is stage 3...","ratingScore":5,"reviewId":"R3DR30FNCR4L70","date":"2024-02-23","reviewReaction":"37","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"The Rebecca Review","productAsin":"1734262419"},{"reviewTitle":"Nice, condensed, informative edition! Highly recommend!","reviewDescription":"I already own a full copy of the full edition of this book...","ratingScore":5,"reviewId":"R6GR56YDZ7P13","date":"2022-06-20","reviewReaction":"27","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"University Doc","productAsin":"1734262419"},{"reviewTitle":"Not for everyone","reviewDescription":"I was (mistakeninly) diagnosed with stage 3 Chronic Kidney Disease or CKD3...","ratingScore":3,"reviewId":"R22BICM5J4CYIK","date":"2022-07-20","reviewReaction":"68","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Crzyoldone","productAsin":"1734262419"},{"reviewTitle":"Stop kidney challenges by changing your diet","reviewDescription":"This is the first book that the information is easy to follow...","ratingScore":5,"reviewId":"R2X35O3KIB4OSU","date":"2023-03-21","reviewReaction":"15","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Amazon Customer","productAsin":"1734262419"},{"reviewTitle":"Great book! Just what you need!","reviewDescription":"Great book if you or a loved one needs to pay diligent attention to kidney health...","ratingScore":5,"reviewId":"R8W60GGDDZ6TM","date":"2023-08-18","reviewReaction":"10","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Laura Lewis","productAsin":"1734262419"},{"reviewTitle":"It was a simple book that explained kidney disease.","reviewDescription":"It was a good book that explains information about kidney disease...","ratingScore":5,"reviewId":"RBJ8CG24ENSWS","date":"2025-08-19","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Diane Blevins","productAsin":"1734262419"},{"reviewTitle":"Very understandable","reviewDescription":"I ordered the first Stopping Kidney Disease Book and was really overwhelmed...","ratingScore":5,"reviewId":"R1PBN9YDBW2A91","date":"2022-03-26","reviewReaction":"13","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Rebecca","productAsin":"1734262419"},{"reviewTitle":"Book is mostly promoting two supplements.","reviewDescription":"This book has some good information, but most of the book promotes two supplements...","ratingScore":3,"reviewId":"R6X7EVMS64B7I","date":"2025-03-07","reviewReaction":"11","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Amazon Customer","productAsin":"1734262419"},{"reviewTitle":"not impressed","reviewDescription":"Over and over it says to be blah blah to be educated...","ratingScore":2,"reviewId":"RKF4FKH8BVJ42","date":"2025-08-01","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"MJ","productAsin":"1734262419"},{"reviewTitle":"Not what I needed","reviewDescription":"The book doesn't have much useful information...","ratingScore":2,"reviewId":"R1E0ZXGB63KCPF","date":"2024-07-22","reviewReaction":"3","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Freda Gaddy","productAsin":"1734262419"},{"reviewTitle":"Not for the average joe","reviewDescription":"Too technical","ratingScore":1,"reviewId":"R121K6TK82IVIU","date":"2025-06-23","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Lorrie V.","productAsin":"1734262419"},{"reviewTitle":"disappointed in hte lack of information","reviewDescription":"This title served more as an ad for a product rather than info on kidney disease.","ratingScore":1,"reviewId":"R3LKIZ9OD44KA4","date":"2024-09-02","reviewReaction":"5","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"SFB","productAsin":"1734262419"},{"reviewTitle":"It's an advertisement","reviewDescription":"I didn't realize that it was actually an advertisement for products the author developed...","ratingScore":1,"reviewId":"R3LCXVR12ZUCYZ","date":"2022-06-12","reviewReaction":"15","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"S. McCloud","productAsin":"1734262419"},{"reviewTitle":"Not worth it","reviewDescription":"This book is a sales pitch for Albutrix, I wish I could return it get a book with better information","ratingScore":1,"reviewId":"R2QFQ2ZN3GWIA3","date":"2023-03-13","reviewReaction":"7","isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":"KS","productAsin":"1734262419"}]

# Transplantation Gp6hGgMywLtxfRq9g (inline, no username)
DS_Gp6hGgMywLtxfRq9g = [{"reviewTitle":"Extremely helpful resource","reviewDescription":"I ordered this book for my father who is experiencing kidney failure...","ratingScore":5,"reviewId":"R1S84E1RCKF1YI","date":"2022-01-25","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Very helpful","reviewDescription":"The scariest part of dealing with chronic disease...","ratingScore":5,"reviewId":"R38UMPIXOWKV89","date":"2020-01-02","reviewReaction":"5","isVerified":True,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Informative, short and easy read","reviewDescription":"This book is a superb simple but very informative book...","ratingScore":5,"reviewId":"R34VR9NOO5ALO9","date":"2021-01-01","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Short read and informative","reviewDescription":"A short read but quite informative on the subject matter...","ratingScore":5,"reviewId":"R2DQTXQVHLBEJ7","date":"2020-06-07","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"A very well written book!!!","reviewDescription":"With its easy to understand language, this book is a go to book...","ratingScore":5,"reviewId":"R1RQR84I6L1854","date":"2020-02-04","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"excellent resource - highly recommend","reviewDescription":"What a great resource for patients and families going through this process!...","ratingScore":5,"reviewId":"RU18TVA7ODMT","date":"2020-02-20","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Very well written book!","reviewDescription":"Well written facts in easy language for patients...","ratingScore":5,"reviewId":"R3KJ8JAD28HAW8","date":"2020-01-04","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Easy to understand","reviewDescription":"Took awhile to receive, but got it","ratingScore":5,"reviewId":"R1FU0X6Q7MRYOW","date":"2020-05-22","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Excellent book","reviewDescription":"Very well explained, didactic Very helpful for our patients","ratingScore":5,"reviewId":"RT0RE339CAQ0N","date":"2020-12-20","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Excellent","reviewDescription":"Being a daughter of a kidney transplant patient...","ratingScore":5,"reviewId":"RE3KIVCFJSLD","date":"2020-02-03","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"An Excellent Resource","reviewDescription":"A very well written book!! Easy to understand and thorough...","ratingScore":5,"reviewId":"R22H3OZJUQTKLS","date":"2020-01-31","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Kindle","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Very Helpful","reviewDescription":"This is excellent book for patient with CKD or who had transplant !!!!","ratingScore":5,"reviewId":"R14SWGXRXOSECF","date":"2020-01-30","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"An excellent book/ guide for both patients , doctors and health care workers.","reviewDescription":"Kidney disease is an epidemic nowadays...","ratingScore":5,"reviewId":"R33CWJASLN3DQZ","date":"2020-01-20","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Excellent book!! Highly recommended for patients as well as health care providers.","reviewDescription":"Excellent Guide for patients...","ratingScore":5,"reviewId":"R5HDDMSN7FF30","date":"2020-01-09","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"An excellent resource for kidney patients.","reviewDescription":"Extremely well written!!...","ratingScore":5,"reviewId":"RRJ6U3WFMN21S","date":"2020-01-08","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":"Format: Paperback","username":None,"productAsin":"B082S6ZRVB"},{"reviewTitle":"Factual Guide for patients who are considering transplants","reviewDescription":"Easy to read, color charts and excellent general guide...","ratingScore":5,"reviewId":"R2VD9P9Q57C6OX","date":"2020-01-02","reviewReaction":None,"isVerified":False,"isAmazonVine":False,"variant":None,"username":None,"productAsin":"B082S6ZRVB"}]

# Transplantation rQG02LiVB7SaSM4Hz (inline, has username)
DS_rQG02LiVB7SaSM4Hz = [{"reviewTitle":"Extremely helpful resource","reviewDescription":"I ordered this book for my father who is experiencing kidney failure...","ratingScore":5,"reviewId":"R1S84E1RCKF1YI","date":"2022-01-25","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Adam","productAsin":"B082S6ZRVB"},{"reviewTitle":"Very helpful","reviewDescription":"The scariest part of dealing with chronic disease...","ratingScore":5,"reviewId":"R38UMPIXOWKV89","date":"2020-01-02","reviewReaction":"5","isVerified":True,"isAmazonVine":False,"variant":None,"username":"Sylvia","productAsin":"B082S6ZRVB"},{"reviewTitle":"Informative, short and easy read","reviewDescription":"This book is a superb simple but very informative book...","ratingScore":5,"reviewId":"R34VR9NOO5ALO9","date":"2021-01-01","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"S Regmi","productAsin":"B082S6ZRVB"},{"reviewTitle":"Short read and informative","reviewDescription":"A short read but quite informative on the subject matter...","ratingScore":5,"reviewId":"R2DQTXQVHLBEJ7","date":"2020-06-07","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":None,"username":"SS","productAsin":"B082S6ZRVB"},{"reviewTitle":"A very well written book!!!","reviewDescription":"With its easy to understand language...","ratingScore":5,"reviewId":"R1RQR84I6L1854","date":"2020-02-04","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":None,"username":"Kusum","productAsin":"B082S6ZRVB"},{"reviewTitle":"excellent resource - highly recommend","reviewDescription":"What a great resource for patients and families going through this process!...","ratingScore":5,"reviewId":"RU18TVA7ODMT","date":"2020-02-20","reviewReaction":"2","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"nsch","productAsin":"B082S6ZRVB"},{"reviewTitle":"Very well written book!","reviewDescription":"Well written facts in easy language for patients...","ratingScore":5,"reviewId":"R3KJ8JAD28HAW8","date":"2020-01-04","reviewReaction":None,"isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Annie","productAsin":"B082S6ZRVB"},{"reviewTitle":"Easy to understand","reviewDescription":"Took awhile to receive, but got it","ratingScore":5,"reviewId":"R1FU0X6Q7MRYOW","date":"2020-05-22","reviewReaction":"1","isVerified":True,"isAmazonVine":False,"variant":"Format: Paperback","username":"Debra Keith","productAsin":"B082S6ZRVB"}]

# Webdatalabs Food Guide hSSKyG8Nbsj7bKuQa (no reviewId)
DS_hSSKyG8Nbsj7bKuQa = [{"productAsin":"0578493624","rating":5,"title":"Delicious Easy Recipes with Great Flavor and Healthy Ingredients!","text":"This cookbook has delicious recipes that are easy to make...","verifiedPurchase":False,"authorName":"The Rebecca Review","reviewDate":"2024-03-13T00:00:00.000Z","helpfulVotes":65,"variant":"Format: Paperback"},{"productAsin":"0578493624","rating":5,"title":"This cookbook has changed my life","text":"This is an excellent cookbook for CKD patients on low protein diets...","verifiedPurchase":False,"authorName":"Lisa C","reviewDate":"2026-01-02T00:00:00.000Z","helpfulVotes":9,"variant":"Format: Paperback"},{"productAsin":"0578493624","rating":4,"title":"Adjustment. Healthy and tasty","text":"I am a foodie...","verifiedPurchase":False,"authorName":"Kindle Customer","reviewDate":"2025-02-07T00:00:00.000Z","helpfulVotes":2,"variant":"Format: Kindle"},{"productAsin":"0578493624","rating":0,"title":"It's ok but..","text":"This will be a great book for our US cousins but the recipes aren't that great for those of us in the UK...","verifiedPurchase":False,"authorName":"moonbeam","reviewDate":"2025-10-20T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Paperback"},{"productAsin":"0578493624","rating":0,"title":"Partner of stage 5 kidney failure man","text":"My husband is at end stage...","verifiedPurchase":False,"authorName":"Aliglaz","reviewDate":"2019-06-28T00:00:00.000Z","helpfulVotes":6,"variant":None},{"productAsin":"0578493624","rating":0,"title":"Great work done for humanity","text":"Namaste ,iam from Maharashtra India...","verifiedPurchase":False,"authorName":"Nilesh  Sonawane","reviewDate":"2025-12-16T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Kindle"},{"productAsin":"0578493624","rating":0,"title":"Good quality recipes for CKD","text":"Some good recipes and info...","verifiedPurchase":False,"authorName":"Rhonda","reviewDate":"2026-01-26T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Paperback"},{"productAsin":"0578493624","rating":0,"title":"Good Food Guide","text":"To cut down on shipping costs, I purchased the two books written by Lee Hull...","verifiedPurchase":False,"authorName":"Godly","reviewDate":"2024-06-07T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Paperback"}]

# Webdatalabs Stopping KD rKpQAYvHvhFgyGN4j (no reviewId)
DS_rKpQAYvHvhFgyGN4j = [{"productAsin":"0692901159","rating":5,"title":"Challenging to read, but a real eye-opener!","text":"Hi everyone! -- I have been a type 2 diabetic for 25 years now...","verifiedPurchase":False,"authorName":"Jerry Ahearn","reviewDate":"2019-09-23T00:00:00.000Z","helpfulVotes":157,"variant":"Format: Kindle"},{"productAsin":"0692901159","rating":5,"title":"You Probably Need This Book","text":"Kidney disease is on the rise, and is a silent killer...","verifiedPurchase":False,"authorName":"Positive Thinker","reviewDate":"2019-08-30T00:00:00.000Z","helpfulVotes":145,"variant":"Format: Paperback"},{"productAsin":"0692901159","rating":0,"title":"Most updated information's on the CKD .","text":"I am in stage 3 of CKD, Bought this book couple of weeks ago...","verifiedPurchase":False,"authorName":"Aizaz","reviewDate":"2023-11-20T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Paperback"},{"productAsin":"0692901159","rating":0,"title":"Very helpful","text":"I bought it for a relative who has CKD...","verifiedPurchase":False,"authorName":"Candy","reviewDate":"2019-08-31T00:00:00.000Z","helpfulVotes":5,"variant":"Format: Kindle"},{"productAsin":"0692901159","rating":0,"title":"A must have for anyone with CKD!","text":"This book is a goldmine of information...","verifiedPurchase":False,"authorName":"Phil Shulkind","reviewDate":"2019-01-28T00:00:00.000Z","helpfulVotes":3,"variant":"Format: Paperback"},{"productAsin":"0692901159","rating":0,"title":"Must read for all kidney patients","text":"Very informative ,inspiring and helpful...","verifiedPurchase":False,"authorName":"MOHAN LAL MUKHI","reviewDate":"2019-07-16T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Kindle"},{"productAsin":"0692901159","rating":0,"title":"Big Book","text":"To cut down on shipping costs, I purchased the two books written by Lee Hull...","verifiedPurchase":False,"authorName":"Godly","reviewDate":"2024-06-07T00:00:00.000Z","helpfulVotes":0,"variant":"Format: Paperback"}]


# ── Load large files ──────────────────────────────────────────────────────────

def load_file(path):
    with open(path) as f:
        return json.load(f)

def load_dataset_file(path):
    data = load_file(path)
    return data.get("items", [])

# Map file paths
FILE_MAP = {
    "KjvhKsYnKzpbO4FbH": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194320096.txt",
    "jT5fiM7ohNz7BGc5m": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194320939.txt",
    "XsomMh3LgQjnGxqjL": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194321703.txt",
    "ePPF5obZCxedHynLE": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194326370.txt",
    "SSSgmzHLPwtanlFl1": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194327257.txt",
    "qhci82TyVboq7TW1y_0": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194336032.txt",
    "qhci82TyVboq7TW1y_200": f"{TOOL_DIR}/mcp-apify-get-actor-output-1776194353121.txt",
    "S4kHGNM3dsWnRpvbn": f"{TOOL_DIR}/toolu_012Fj8AvxTvogPG9cp3SJaq9.txt",
}


# ── Normalization helpers ─────────────────────────────────────────────────────

def safe_int(v, default=0):
    if v is None:
        return default
    try:
        return int(str(v).split()[0])
    except:
        return default

def parse_axesso_rating(s):
    """'5.0 out of 5 stars' → 5"""
    if s is None:
        return None
    m = re.match(r"(\d+\.?\d*)", str(s))
    return int(float(m.group(1))) if m else None

def parse_axesso_date(s):
    """'Reviewed in the United States on January 5, 2026' → '2026-01-05'"""
    if not s:
        return ""
    m = re.search(r"on (.+)$", s)
    if not m:
        return s
    try:
        return datetime.strptime(m.group(1).strip(), "%B %d, %Y").strftime("%Y-%m-%d")
    except:
        return s

def parse_wdl_date(s):
    """'2024-03-13T00:00:00.000Z' → '2024-03-13'"""
    if not s:
        return ""
    return s[:10]

def normalize_junglee(item, source="junglee"):
    asin = item.get("productAsin", "")
    return {
        "reviewId": item.get("reviewId"),
        "asin": asin,
        "book": ASIN_TO_BOOK.get(asin, asin),
        "source": source,
        "date": item.get("date", ""),
        "rating": item.get("ratingScore"),
        "verified": item.get("isVerified", False),
        "helpfulVotes": safe_int(item.get("reviewReaction")),
        "author": item.get("username", ""),
        "title": item.get("reviewTitle", ""),
        "text": item.get("reviewDescription", ""),
        "format": item.get("variant", ""),
        "vine": item.get("isAmazonVine", False),
        "flag": "",
    }

def normalize_axesso(item):
    asin = item.get("asin", "")
    rating_raw = item.get("rating")
    if isinstance(rating_raw, str):
        rating = parse_axesso_rating(rating_raw)
    elif isinstance(rating_raw, (int, float)):
        rating = int(rating_raw)
    else:
        rating = None
    date_raw = item.get("date", "")
    date = parse_axesso_date(date_raw) if "on " in str(date_raw) else str(date_raw)[:10]
    var_list = item.get("variationList", [])
    fmt = ", ".join(str(v) for v in var_list) if isinstance(var_list, list) else str(var_list)
    return {
        "reviewId": item.get("reviewId"),
        "asin": asin,
        "book": ASIN_TO_BOOK.get(asin, asin),
        "source": "axesso",
        "date": date,
        "rating": rating,
        "verified": item.get("verified", False),
        "helpfulVotes": safe_int(item.get("numberOfHelpful")),
        "author": item.get("userName", ""),
        "title": item.get("title", ""),
        "text": item.get("text", ""),
        "format": fmt,
        "vine": item.get("vine", False),
        "flag": "",
    }

def normalize_webdatalabs(item):
    asin = item.get("productAsin", "")
    rating = item.get("rating", 0)
    return {
        "reviewId": None,
        "asin": asin,
        "book": ASIN_TO_BOOK.get(asin, asin),
        "source": "webdatalabs",
        "date": parse_wdl_date(item.get("reviewDate", "")),
        "rating": rating,
        "verified": item.get("verifiedPurchase", False),
        "helpfulVotes": safe_int(item.get("helpfulVotes")),
        "author": item.get("authorName", ""),
        "title": item.get("title", ""),
        "text": item.get("text", ""),
        "format": item.get("variant", ""),
        "vine": False,
        "flag": "",
    }


# ── Main processing ───────────────────────────────────────────────────────────

master = {}   # reviewId → record
wdl_records = []  # webdatalabs records (no reviewId)

def add_junglee(items, source="junglee"):
    for item in items:
        if item.get("error"):
            continue
        rid = item.get("reviewId")
        if not rid:
            continue
        norm = normalize_junglee(item, source)
        if rid not in master:
            master[rid] = norm
        else:
            # prefer junglee; update username if missing
            existing = master[rid]
            if not existing.get("author") and norm.get("author"):
                existing["author"] = norm["author"]
            # update helpful votes if larger
            if norm["helpfulVotes"] > existing["helpfulVotes"]:
                existing["helpfulVotes"] = norm["helpfulVotes"]

def add_axesso(items):
    for item in items:
        if item.get("error"):
            continue
        rid = item.get("reviewId")
        if not rid:
            continue
        if rid not in master:
            norm = normalize_axesso(item)
            master[rid] = norm

def add_webdatalabs(items):
    for item in items:
        norm = normalize_webdatalabs(item)
        wdl_records.append(norm)


# Load all junglee datasets
print("Loading junglee datasets...")
add_junglee(DS_4PffBeuoEsod3CZrY)
add_junglee(DS_KUBiW1pBhtYvS2u0y)

# Stopping KD junglee from files
for key in ["KjvhKsYnKzpbO4FbH", "jT5fiM7ohNz7BGc5m", "XsomMh3LgQjnGxqjL"]:
    print(f"  Loading {key}...")
    items = load_dataset_file(FILE_MAP[key])
    add_junglee(items)

# Food Guide junglee from files
for key in ["ePPF5obZCxedHynLE", "SSSgmzHLPwtanlFl1"]:
    print(f"  Loading {key}...")
    items = load_dataset_file(FILE_MAP[key])
    add_junglee(items)

add_junglee(DS_ZRtyz4Sxp4S4zrEJ5)
add_junglee(DS_5L9W8RcCky1diBaMa)

# Basics junglee from file + inline
print("  Loading S4kHGNM3dsWnRpvbn...")
basics_file = load_dataset_file(FILE_MAP["S4kHGNM3dsWnRpvbn"])
add_junglee(basics_file)
add_junglee(DS_OTaP0rqmtmca4or9B)

# Transplantation junglee inline
add_junglee(DS_Gp6hGgMywLtxfRq9g)
add_junglee(DS_rQG02LiVB7SaSM4Hz)

print(f"After junglee: {len(master)} unique reviews")

# Load axesso dataset
print("Loading axesso dataset...")
axesso_items = load_dataset_file(FILE_MAP["qhci82TyVboq7TW1y_0"])
axesso_items += load_dataset_file(FILE_MAP["qhci82TyVboq7TW1y_200"])
add_axesso(axesso_items)
print(f"After axesso: {len(master)} unique reviews")

# Webdatalabs
print("Processing webdatalabs...")
add_webdatalabs(DS_hSSKyG8Nbsj7bKuQa)
add_webdatalabs(DS_rKpQAYvHvhFgyGN4j)

# Match webdatalabs by (title, author)
title_author_index = {}
for rid, rec in master.items():
    t = (rec["title"] or "").strip().lower()[:80]
    a = (rec["author"] or "").strip().lower()[:30]
    title_author_index[(t, a)] = rid

wdl_new = 0
wdl_matched = 0
wdl_rating_fixed = 0
for wdl in wdl_records:
    key = ((wdl["title"] or "").strip().lower()[:80], (wdl["author"] or "").strip().lower()[:30])
    if key in title_author_index:
        # Matched — skip, but fix rating=0 if needed
        wdl_matched += 1
        existing_rid = title_author_index[key]
        existing = master[existing_rid]
        if (existing.get("rating") == 0 or existing.get("rating") is None) and wdl["rating"] and wdl["rating"] > 0:
            existing["rating"] = wdl["rating"]
            wdl_rating_fixed += 1
    else:
        # New record
        wdl_new += 1
        flag = "NEEDS_REVIEW"
        if wdl["rating"] == 0:
            flag = "RATING_UNKNOWN"
        wdl["flag"] = flag
        # Give it a synthetic key
        synthetic_id = f"WDL_{wdl['asin']}_{wdl_new:04d}"
        master[synthetic_id] = wdl

print(f"WDL matched: {wdl_matched}, fixed rating: {wdl_rating_fixed}, new: {wdl_new}")
print(f"Total unique reviews: {len(master)}")

# ── Organize by ASIN ──────────────────────────────────────────────────────────

by_asin = defaultdict(list)
for rec in master.values():
    asin = rec["asin"]
    if asin in ASIN_TO_BOOK:
        by_asin[asin].append(rec)

# Sort by date descending
for asin in by_asin:
    by_asin[asin].sort(key=lambda r: r["date"] or "", reverse=True)

# ── Print summary ─────────────────────────────────────────────────────────────

print("\n=== SUMMARY ===")
total_scraped = 0
for asin, book in ASIN_TO_BOOK.items():
    n = len(by_asin[asin])
    target = ASIN_TARGETS[asin]
    pct = n / target * 100
    total_scraped += n
    print(f"{book[:50]:50s}: {n:3d}/{target} ({pct:.0f}%)")
print(f"\nTotal: {total_scraped}/536")

# Flagged reviews
flagged = [r for r in master.values() if r.get("flag")]
print(f"\nFlagged reviews: {len(flagged)}")
for r in flagged:
    print(f"  [{r['flag']}] {r['author']} | {r['title'][:50]} | {r['date']}")

# ── Export CSVs ───────────────────────────────────────────────────────────────

FIELDNAMES = ["reviewId","asin","book","source","date","rating","verified","helpfulVotes","author","title","text","format","vine","flag"]

os.makedirs("reviews_data", exist_ok=True)

FILE_NAMES = {
    "B0DM96NVSX": "reviews_data/evidence_based_guide.csv",
    "0692901159": "reviews_data/stopping_kidney_disease.csv",
    "0578493624": "reviews_data/food_guide.csv",
    "1734262419": "reviews_data/basics.csv",
    "B082S6ZRVB": "reviews_data/transplantation.csv",
}

for asin, fname in FILE_NAMES.items():
    rows = by_asin[asin]
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows → {fname}")

print("\nDone!")

# Save counts for commit message
counts = {asin: len(by_asin[asin]) for asin in ASIN_TO_BOOK}
with open("reviews_data/counts.json", "w") as f:
    json.dump(counts, f, indent=2)
