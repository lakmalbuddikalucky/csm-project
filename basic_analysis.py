import pandas as pd
import seaborn as sns
import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import Counter
stopword_list = list(set(stopwords.words('english')))

# populate lists with handles from
dataset = pd.read_csv("./tweet_dataset.csv")
# drop duplicates
dataset = dataset.drop_duplicates(subset=['TWEET_ID'])

# create dataframes for each category
reps = dataset.loc[dataset["PARTY"] == 'Rep']
dems = dataset.loc[dataset["PARTY"] == 'Dem']
rep_politicians = reps.loc[reps["TYPE"] == "Politician"]
rep_celebs = reps.loc[reps["TYPE"] == "Celebrity"]
rep_media = reps.loc[reps["TYPE"] == "Media"]
dem_politicians = dems.loc[dems["TYPE"] == "Politician"]
dem_celebs = dems.loc[dems["TYPE"] == "Celebrity"]
dem_media = dems.loc[dems["TYPE"] == "Media"]


# boxplots for retweet counts
g = sns.catplot(x="PARTY", y="RETWEET_COUNT", col='TYPE', data=dataset,
                kind="box", col_wrap=3, legend_out=False, palette="husl", sharey=False, showfliers = False)


# boxplots for retweet counts
g = sns.catplot(x="PARTY", y="FAVORITE_COUNT", col='TYPE', data=dataset,
                kind="box", col_wrap=3, legend_out=False, palette="husl", sharey=False, showfliers = False)

# count plots for number of tweets
g = sns.catplot(x="PARTY",col='TYPE', data=dataset,
                kind="count", col_wrap=3, legend_out=False, palette="Paired")
for ax in g.axes.flat:
    for label in ax.get_xticklabels():
        label.set_rotation(90)

# plot for URLS
grid = sns.FacetGrid(data=dataset,col='TYPE', row="PARTY")
fig = grid.map(sns.countplot,'NUM_OF_URLS', palette='Set1')








#
# preprocessing to obtain hashtags and 
#

party = []
type = []
words = []
hashtags = []
# iterate through tweets dataset and calculate statistics
for index, row in dataset.iterrows():
    tweet_text = row["TEXT"]
    party_name = row["PARTY"]
    type_name = row["TYPE"]
    hashtag_list = []

    # convert letters to lower case
    tweet_text_processed = tweet_text.lower()
    # break senstences into words
    tweet_word_list = tweet_text_processed.split()
    # filter out hashtags
    for element in tweet_word_list:
        if(element[0] == '#'):
            hashtag_list.append(element)
    print(hashtag_list)

    # remove unwanted characters
    text = re.sub(r'\W+', ' ', tweet_text_processed)
    text_list = text.split()
    # remove shorts words and stop words
    remove_list = []
    for element in text_list:
        if(len(element) <= 2): remove_list.append(element)
        if(element[0:4] == "http"): remove_list.append(element)
        if(element in stopword_list): remove_list.append(element)
    text_list_wo_short_words = [ele for ele in text_list if ele not in remove_list]

    # lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_list = []
    for element in text_list_wo_short_words:
        lematized = lemmatizer.lemmatize(element)
        lemmatized_list.append(lematized)

    party.append(party_name)
    type.append(type_name)
    hashtags.append(hashtag_list)
    words.append(lemmatized_list)







# create new dataset with hashtags and words
df = pd.DataFrame(list(zip(party, type, hashtags, words)), columns =['PARTY', 'TYPE', 'HASHTAGS', 'WORDS'])
# create dataframes for each category
reps_df = df.loc[df["PARTY"] == 'Rep']
dems_df = df.loc[df["PARTY"] == 'Dem']
rep_politicians_df = reps_df.loc[reps_df["TYPE"] == "Politician"]
rep_celebs_df = reps_df.loc[reps_df["TYPE"] == "Celebrity"]
rep_media_df = reps_df.loc[reps_df["TYPE"] == "Media"]
dem_politicians_df = dems_df.loc[dems_df["TYPE"] == "Politician"]
dem_celebs_df = dems_df.loc[dems_df["TYPE"] == "Celebrity"]
dem_media_df = dems_df.loc[dems_df["TYPE"] == "Media"]







#
# this creates a dataset of hashtag for a given part and type
#
def create_dataset_of_hashtags(dataset, party_name, type_name):
    number_of_elements = 10
    counts = Counter(dataset.HASHTAGS.sum())
    counts_df = pd.DataFrame.from_dict(counts, orient='index')
    counts_df = counts_df.reset_index()
    counts_df.columns = ["hashtag","count"]
    counts_df = counts_df.sort_values(by=["count"], ascending=False)
    counts_df_top_20 = counts_df.head(number_of_elements)
    counts_df_top_20["PARTY"] = [party_name]*number_of_elements
    counts_df_top_20["TYPE"] = [type_name]*number_of_elements
    return counts_df_top_20







#
# this creates a dataset of hashtag for a given part and type
#
def create_dataset_of_words(dataset, party_name, type_name):
    number_of_elements = 10
    counts = Counter(dataset.WORDS.sum())
    counts_df = pd.DataFrame.from_dict(counts, orient='index')
    counts_df = counts_df.reset_index()
    counts_df.columns = ["word","count"]
    counts_df = counts_df.sort_values(by=["count"], ascending=False)
    counts_df_top_20 = counts_df.head(number_of_elements)
    counts_df_top_20["PARTY"] = [party_name]*number_of_elements
    counts_df_top_20["TYPE"] = [type_name]*number_of_elements
    return counts_df_top_20








#
# Get diagram for hashtag counts
#
rep_politicians_ht = create_dataset_of_hashtags(rep_politicians_df, "Rep", "Politician")
rep_celebs_ht = create_dataset_of_hashtags(rep_celebs_df, "Rep", "Celebrity")
rep_media_ht = create_dataset_of_hashtags(rep_media_df, "Rep", "Media")
dem_politicians_ht = create_dataset_of_hashtags(dem_politicians_df, "Dem", "Politician")
dem_celebs_ht = create_dataset_of_hashtags(dem_celebs_df, "Dem", "Celebrity")
dem_media_ht = create_dataset_of_hashtags(dem_media_df, "Dem", "Media")

hashtag_dataset = pd.concat([rep_politicians_ht, rep_celebs_ht, rep_media_ht, dem_politicians_ht, dem_celebs_ht, dem_media_ht])
# count plots for number of hashtags
g = sns.catplot(x="hashtag", y="count", col='TYPE', row="PARTY", data=hashtag_dataset,
                kind="bar", legend_out=False, palette="Paired", sharey=False, sharex=False)
for ax in g.axes.flat:
    for label in ax.get_xticklabels():
        label.set_rotation(90)



#
# Get diagram for word counts
#
rep_politicians_word = create_dataset_of_words(rep_politicians_df, "Rep", "Politician")
rep_celebs_word = create_dataset_of_words(rep_celebs_df, "Rep", "Celebrity")
rep_media_word = create_dataset_of_words(rep_media_df, "Rep", "Media")
dem_politicians_word = create_dataset_of_words(dem_politicians_df, "Dem", "Politician")
dem_celebs_word = create_dataset_of_words(dem_celebs_df, "Dem", "Celebrity")
dem_media_word = create_dataset_of_words(dem_media_df, "Dem", "Media")

word_dataset = pd.concat([rep_politicians_word, rep_celebs_word, rep_media_word, dem_politicians_word, dem_celebs_word, dem_media_word])
# count plots for number of hashtags
g = sns.catplot(x="word", y="count", col='TYPE', row="PARTY", data=word_dataset,
                kind="bar", legend_out=False, palette="Paired", sharey=False, sharex=False)
for ax in g.axes.flat:
    for label in ax.get_xticklabels():
        label.set_rotation(90)
