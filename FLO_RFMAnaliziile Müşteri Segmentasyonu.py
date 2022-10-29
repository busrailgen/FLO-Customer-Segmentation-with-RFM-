###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# 1. İş Problemi (Business Problem)
###############################################################

# Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

# Veri Seti Hikayesi

# Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında OmniChannel (hem online hem offline alışveriş yapan)
# olarak yapan müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

# Değişkenler
# master_id: Eşsiz müşteri numarası
# order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
# last_order_channel: En son alışverişin yapıldığı kanal
# first_order_date: Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date: Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# Gorev 1: Veriyi Anlama ve Hazırlama
###############################################################

import datetime as dt
import pandas as pd

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

# Adım1: flo_data_20K.csv verisinio kuyunuz.Dataframe’in kopyasını oluşturunuz.
df = pd.read_csv("datasets/flo_data_20k.csv")

# Adım2: Veri setini incele.
df.head(10)
df.columns
df.describe().T
df.isnull().sum()
df.dtypes

# Adım3:Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Her bir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

df["total_transaction"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["total_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

# Adım4:  Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

# Adım5:  Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.
df.groupby("order_channel").agg({"total_transaction": "sum",
                                 "total_price": "sum"})

# Adım6:  En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values("total_price", ascending=False).head(10)

# Adım7:  En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values("total_transaction", ascending=False).head(10)


# Adım8:  Veri ön hazırlık sürecini fonksiyonlaştırınız.

def data_preparation(dataframe):
    dataframe["total_transaction"] = dataframe["order_num_total_ever_online"] + dataframe[
        "order_num_total_ever_offline"]
    dataframe["total_price"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]

    dataframe["first_order_date"] = pd.to_datetime(dataframe["first_order_date"])
    dataframe["last_order_date"] = pd.to_datetime(dataframe["last_order_date"])
    dataframe["last_order_date_online"] = pd.to_datetime(dataframe["last_order_date_online"])
    dataframe["last_order_date_offline"] = pd.to_datetime(dataframe["last_order_date_offline"])
    return dataframe


data_preparation(df)
df.head()

###############################################################
# Gorev 2: RFM Metriklerinin Hesaplanması
###############################################################

#Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
#Recency: Sıcaklık, analiz tarihine göre müşterinin son alışverişi üzerinden geçen zaman
#Frequency: Müşterinin alışveriş sıklığı,toplam yaptığı alışveriş sayısı
#Monetary: Müşterinin alışverişlerinin toplam tutarı

#recencydeğerini hesaplamak için analiz tarihini maksimum tarihten 2 gün sonrası seçebilirsiniz
df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)

#Adım 2: Müşteri özelinde Recency, Frequencyve Monetary metriklerini groupby, aggve lambda ile hesaplayınız.
df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                   'total_price': lambda total_price: total_price,
                                   'total_transaction': lambda total_transaction: total_transaction,
                                   'interested_in_categories_12': lambda
                                       interested_in_categories_12: interested_in_categories_12})

#Adım 3: Hesapladığınız metrikleri rfmisimli bir değişkene atayınız.
rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                   'total_price': lambda total_price: total_price,
                                   'total_transaction': lambda total_transaction: total_transaction,
                                   'interested_in_categories_12': lambda
                                       interested_in_categories_12: interested_in_categories_12})

rfm.head()

#Adım4: Oluşturduğunuz metriklerin isimlerini  recency, frequencyve monetary olarak değiştiriniz.
rfm.columns = ["recency", "monetary", "frequency", "interested_cats"]
rfm.describe().T

###############################################################
# Gorev 3: RFM Skorlarının Oluşturulması ve Tek bir Değişkene Çevrilmesi
###############################################################

#Adım 1: Recency, Frequencyve Monetarymetriklerini qcutyardımı ile 1-5 arasında skorlara çeviriniz.
#Bu skorları recency_score, frequency_scoreve monetary_scoreolarak kaydediniz.
rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

#Adım 2: recency_scoreve frequency_score’utek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.
rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))

###############################################################
# Gorev 4: RF Skorunun SegmentOlarak Tanımlanması
###############################################################

#Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

#Adım 2: Yukarıdaki seg_map yardımı ile skorları segmentlere çeviriniz.
rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

###############################################################
# Gorev 4: Aksiyon Zamanı !
###############################################################

# Adım1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

# Adım2:  RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.

# a.FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde.
# Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçmek isteniliyor.
# Sadık müşterilerinden(champions,loyal_customers) ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kurulacak müşteriler.
# Bu müşterilerin id numaralarını csv dosyasına kaydediniz.

type_a_customers = pd.DataFrame()
type_a_customers = rfm.loc[
    (rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers") & rfm["interested_cats"].str.contains(
        'KADIN')].reset_index()
type_a_customers["type_a_customers_id"] = rfm.loc[
    (rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers") & rfm["interested_cats"].str.contains(
        'KADIN')].index

type_a_customers["type_a_customers_id"].to_csv("type_a_customers.csv")
type_a_customers["master_id"].to_csv("master_id.csv")

# b.Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır.
# Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler,
# uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniyor.
# Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.

type_b_customers = pd.DataFrame()
type_b_customers = rfm.loc[(rfm["segment"] == "cant_loose") | (rfm["segment"] == "hibernating") | (
        rfm["segment"] == "new_customers")].reset_index()
type_b_customers = type_b_customers.loc[(type_b_customers["interested_cats"].str.contains("ERKEK")) | (
    type_b_customers["interested_cats"].str.contains("COCUK"))]

type_b_customers["master_id"].to_csv("type_b_customers.csv")
