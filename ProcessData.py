import pandas as pd

# Read Data / Cleanup Columns
df = pd.read_csv("12thsinglegeresults.csv", sep=",")
df.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in df.columns]
df = df[['no', 'wallet_code', 'answer', 'vote_amount']]
df.columns = ['no', 'wallet', 'member', 'vote_amount']

# Calculate Token (Convert Wei Value)
df['vote_amount'] = df['vote_amount'].astype(float)
df['vote_amount'] = df['vote_amount'] / 10 ** 18

# Feature Engineering - Total Vote / Vote Count of each Wallet
df_wallet = df.groupby(['wallet']).agg({'member': 'nunique', 'vote_amount': 'sum', 'no': 'count'}).reset_index()
df_wallet.columns = ['wallet', 'wallet_unique_member', 'wallet_total_vote_amount', 'wallet_total_vote_count']
df_wallet['wallet_rank'] = df_wallet['wallet_total_vote_amount'].rank(method='first', ascending=False)

# Feature Engineering - Total Vote / Vote Count of each Member
df_member = df.groupby(['member']).agg({'wallet': 'nunique', 'vote_amount': 'sum', 'no': 'count'}).reset_index()
df_member.columns = ['member', 'member_unique_wallet', 'member_total_vote_amount', 'member_total_vote_count']
df_member['member_rank'] = df_member['member_total_vote_amount'].rank(method='first', ascending=False)

# Feature Engineering - Rank of Wallet Voters
df_member_wallet = df.groupby(['member', 'wallet']).agg({'vote_amount': 'sum'}).reset_index()
df_member_wallet.columns = ['member', 'wallet', 'member_wallet_total_vote']
df_member_wallet = df_member_wallet.sort_values(['member', 'member_wallet_total_vote'], ascending=[True, False])
df_member_wallet['member_wallet_rank'] = df_member_wallet.groupby(['member'])['member_wallet_total_vote'].rank(method='first',
                                                                                                               ascending=False)
df_member_sum = df_member[['member', 'member_total_vote_amount']]
df_member_wallet = df_member_wallet.merge(df_member_sum, how='left')
df_member_wallet['member_wallet_contribution'] = (df_member_wallet['member_wallet_total_vote'] / df_member_wallet[
    'member_total_vote_amount']) * 100
df_member_wallet = df_member_wallet.drop('member_total_vote_amount', axis=1)

df_sum = df.copy(deep=True)

df_sum = df_sum.merge(df_wallet, how='left')
df_sum = df_sum.merge(df_member, how='left')
df_sum = df_sum.merge(df_member_wallet, how='left')
df_sum.to_csv("12thgesummary.csv", sep=",", index=False)
