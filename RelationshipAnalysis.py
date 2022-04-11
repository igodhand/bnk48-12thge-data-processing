import pandas as pd
import itertools

# Read Data / Cleanup Columns
df = pd.read_csv("12thsinglegeresults.csv", sep=",")
df.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in df.columns]
df = df[['no', 'wallet_code', 'answer', 'vote_amount']]
df.columns = ['no', 'wallet', 'member', 'vote_amount']

# Calculate Token (Convert Wei Value)
df['vote_amount'] = df['vote_amount'].astype(float)
df['vote_amount'] = df['vote_amount'] / 10 ** 18

# Prepare Wallet Total Dataset
df_wallet = df.groupby(['wallet']).agg({'vote_amount': 'sum'}).reset_index()
df_wallet.columns = ['wallet', 'wallet_total_vote']

# Prepare Wallet x Member Dataset
df_wallet_member = df.groupby(['wallet', 'member']).agg({'vote_amount': 'sum'}).reset_index()
df_wallet_member.columns = ['wallet', 'member', 'vote_amount']
df_wallet_member = df_wallet_member.sort_values(['wallet', 'vote_amount'], ascending=[True, False])

# One-Hot Encoding
df_sum = df_wallet_member.groupby(['wallet', 'member'])['vote_amount'].sum().unstack(fill_value=0).reset_index()
print(df_sum)

# Join Total
df_sum = df_sum.merge(df_wallet, how='left')

# list member-valued column after unstacked
col_group = ['wallet', 'wallet_total_vote']
cat_group = [c for c in df_sum.columns if c not in col_group]

# Finding Permutations
# list_iter - will contain left and right member name
# You can use combinations for only unique match
iter = itertools.permutations(cat_group, 2)
list_iter = []
for i in iter:
    list_iter.append([i[0], i[1]])

# List
list_df = []
for i in list_iter:
    left = i[0]
    right = i[1]
    cols = col_group + i
    df_tmp = df_sum[['wallet', 'wallet_total_vote', left, right]]
    df_tmp = df_tmp[(df_tmp[left] > 0) & (df_tmp[right] > 0)]
    df_tmp['left'] = left
    df_tmp['right'] = right
    df_tmp['left_value'] = df_tmp[left]
    df_tmp['right_value'] = df_tmp[right]
    df_tmp['total'] = df_tmp[left] + df_tmp[right]
    df_tmp['total_vote'] = df_tmp['wallet_total_vote']
    df_tmp['left_contribute'] = df_tmp['left_value'] / df_tmp['wallet_total_vote']
    df_tmp['right_contribute'] = df_tmp['right_value'] / df_tmp['wallet_total_vote']
    df_tmp['total_contribute'] = df_tmp['total'] / df_tmp['wallet_total_vote']
    df_tmp['count'] = 1
    df_tmp = df_tmp.drop([left, right], axis=1)
    print(cols)
    list_df.append(df_tmp)

df_total = pd.concat(list_df, axis=0)
print(df_total)

df_total.to_csv("12thgerelationship.csv", sep=",", index=False)
