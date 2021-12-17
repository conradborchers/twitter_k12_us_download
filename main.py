import requests
import time
import glob
import re
import json
import datetime
import pandas as pd

def read_bearer_token(file_path = 'bearer_token.txt'):
    """
    This is a helper function that reads a bearer token form
    a specified file path into a string object.
    
    Parameters
    ----------
    file_path : str
        Readable file path containing a document which includes
        the Twitter API bearer token in its first line. Defaults
        to 'bearer_token.txt'.
        
    Returns
    -------
    str
        The Twitter API bearer token as a string object.
    """
    # Will raise error if path does not exist
    with open(file_path, "r") as f:
        for line in f:
            BEARER_TOKEN = line.strip()
            break
    f.close() 
    return BEARER_TOKEN

def look_up_twitter_acount_id(BEARER_TOKEN, user_name):
    """
    This is a helper function to set a simple Twitter API 
    request to look up a Twitter user ID based on a user
    handle (e.g., '@twitter' or 'nhsuk'). The user can both
    parse in the username as a handle (string including '@') 
    and as a string without the '@'. The Twitter user ID
    is needed for subsequent Twitter API calls.
    
    GET /2/users/by/username/:username
    
    App rate limit: 300 requests per 15-minute window
    
    https://developer.twitter.com/en/docs/twitter-api/users/lookup/api-reference/get-users-by-username-username
    
    Parameters
    ----------
    BEARER_TOKEN : str
        A Twitter API bearer token as a string object.
    user_name : str
        The username to look up (either including or 
        excluding) the '@' symbol.
        
    Returns
    -------
    str
        The Twitter user ID as a string object.
        
    Raises
    ------
    ApiError
        Either the request status code was not 200
        or the requested user name was malformed.
    """
    # If a handle was parsed, convert to user name
    if (user_name[0] == "@"):
        user_name = user_name[1:]
    
    # If user name does not only contain letters, numbers, or
    # underscores, raise error
    if not re.match("^[\w\d_]*$", user_name):
        raise ApiError('The user name you requested seems to be malformed.')
    
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})

    req = s.get(f'https://api.twitter.com/2/users/by?usernames={user_name}')
    time.sleep(3) # rate limit
    
    if req.status_code != 200:
        raise ApiError(f'There was an error sending the request. '\
                       f'Status code {req.status_code}')
    
    page = json.loads(req.content)

    twitter_id = page['data'][0]['id']
    
    return twitter_id

def get_most_recent_tweets_account(ACCOUNT_ID, BEARER_TOKEN, PARAMS, 
                                   verbose=True, save_file=True,
                                   file_reference='DOWNLOAD'):
    """
    This function is a download routine to get the most recent 
    Tweets of a specified Twitter account. It extracts the pagination 
    from the results of the API calls until either less than 100 
    results are returned or 32 API calls have been made (which is 
    the maximum number of tweets Twitter allows academic researchers 
    to download as of November 2021). The function uses the 
    GET /2/users/:id/tweets endpoint of the Twitter V2 API. The 
    endpoint allows for 900 requests per 15-minute window. Hence, 
    the thread pauses for 1 second in between API calls. More 
    information on the endpoint can be found here:
    https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/api-reference/get-users-id-tweets
    
    Parameters
    ----------
    ACCOUNT_ID : str
        A Twitter user ID.
    BEARER_TOKEN : str
        A Twitter API bearer token.
    PARAMS : dict
        A dictionary parsed to the header of the API URL request
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
    file_reference : str
        Reference string that should appear in the output
        file name if the results should be saved.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
        
    Raises
    ------
    ApiError
        Either the request status code was not 200
        or the PARAMS argument is malformed (but not
        invalid) to ensure that the pagination routine
        works as intended.
    """
    if 'pagination_token' in PARAMS.keys():
        raise ApiError('The parsed query parameters included a pagination '\
                       'token. Check your PARAMS argument.')
        
    if 'max_results' not in PARAMS.keys() or int(PARAMS['max_results']) != 100:
        raise ApiError('Please ensure that you parse max_results: 100 to '\
                       'your requests parameters.')
    
    # Prepare URL request
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})
    URL = f"https://api.twitter.com/2/users/{ACCOUNT_ID}/tweets"
    request_count = 0
    
    while (request_count < 32):
        req = requests.models.PreparedRequest()
        req.prepare_url(URL, PARAMS)
        req = s.get(req.url)
        # Sleep for 1 second due to rate limit
        time.sleep(1)
        
        if req.status_code != 200:
            raise ApiError(f'There was an error sending request '\
                           f'{request_count+1}. Status code '\
                           f'{req.status_code}')

        # Get content, paginate and save rows to df
        page = json.loads(req.content)
        
        if page['meta']['result_count'] == 0 and request_count == 0:
            if verbose:
                print(f'No results found for {file_reference}! Returning '\
                      f'empty data frame.')
            if save_file:
                print('Note: Results will not be written to a '\
                      'timestamped file but an empty file will '\
                      'be created for future reference.')
                open(f'data/{file_reference}_2021_empty.csv', 'a').close()
            return pd.DataFrame()
        
        if 'data' not in page.keys():
            if verbose:
                print(f'All tweets for {ACCOUNT_ID} found after '\
                      f'{request_count+1} API calls.')
            break
        
        # If it is the first request, initialize data frame
        if request_count == 0: 
            df = pd.json_normalize(page['data'])
        # Else append results to existing data frame
        else:
            df = df.append(pd.json_normalize(page['data']))
        
        if 'next_token' not in page['meta'].keys():
            if verbose:
                print(f'All tweets for {ACCOUNT_ID} found after '\
                      f'{request_count+1} API calls.')
            break
        
        # Update pagination token and request_count
        NEXT_TOKEN = page['meta']['next_token']
        PARAMS['pagination_token'] = NEXT_TOKEN
        request_count += 1
        
        if verbose: 
            print(f'Request {request_count} successful. Sleeping 1 '\
                  f'second and paginating.')
        
    if verbose:
        print(f'All most recent for account {file_reference} downloaded.')
        
    if save_file:
        # Save file with timestamp
        fn = f"data/{file_reference}_"\
             f"{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        print(f'Saving file to {fn}')
        df.to_csv(fn, index=False)
        
    return df

def download_and_save_account_tweets(token_file_path='bearer_token.txt', 
                                   user_name='nhsuk', verbose=True, 
                                   save_file=True): 
    """
    This is a wrapper function for the
    get_most_recent_tweets_account() function to download 
    the most recent tweets of a specified Twitter account
    through the Twitter API. The request parameters
    (i.e. variable requested) are hard-coded in this
    function for this study. For more information 
    go to the documentation of the get_most_recent_tweets_account() 
    function via help(get_most_recent_tweets_account).
    
    Parameters
    ----------
    token_file_path : str
        Readable file path containing a document which includes
        the Twitter API bearer token in its first line. Defaults
        to 'bearer_token.txt'.
    user_name : str
        The username to look up (either including or 
        excluding) the '@' symbol.
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
    
    """
    BEARER_TOKEN = read_bearer_token(token_file_path)
    TWITTER_USER_ID = look_up_twitter_acount_id(BEARER_TOKEN, user_name)
    
    # Download all variables currently available
    PARAMS = {
        "max_results": "100", # maximum number of results permitted
        "tweet.fields": "attachments,author_id,context_annotations,conversation_id,"\
                        "created_at,entities,geo,id,in_reply_to_user_id,lang,"\
                        #"non_public_metrics,"\ # not available
                        "public_metrics,"\
                        # "organic_metrics,"\  # not available
                        # "promoted_metrics,"\ # not available
                        "possibly_sensitive,referenced_tweets,"\
                        "reply_settings,source,text,withheld", 
        "user.fields": "created_at,description,entities,id,location,name,"\
                       "pinned_tweet_id,profile_image_url,protected,public_metrics,"\
                        "url,username,verified,withheld",
        "expansions":  "attachments.poll_ids,attachments.media_keys,"\
                       "author_id,entities.mentions.username,geo.place_id,"\
                       "in_reply_to_user_id,referenced_tweets.id,"\
                       "referenced_tweets.id.author_id",
        "media.fields": "duration_ms,height,media_key,preview_image_url,type,"\
                        "url,width,"\
                        "public_metrics,"\
                        #"non_public_metrics,"\ # not available
                        #"organic_metrics,promoted_metrics,"\ # not available
                        "alt_text",
        "place.fields": "contained_within,country,country_code,full_name,"\
                        "geo,id,name,place_type",
        "poll.fields": "duration_minutes,end_datetime,id,options,voting_status"  
    }

    df = get_most_recent_tweets_account(TWITTER_USER_ID, BEARER_TOKEN, 
                                        PARAMS, verbose=verbose, 
                                        save_file=save_file,
                                        file_reference=user_name)
    return df

def extract_username_from_url(text: str):
    # remove everthing prior to and including twitter.com/
    text = text.split('twitter.com/')[-1] 
    # get everthing prior to first / after twitter.com
    text = text.split('/')[0]
    # clean names
    PERMITTED_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
    text = "".join(char for char in text if char in PERMITTED_CHARS)
    return text

if __name__ == '__main__':
    df = pd.read_csv('twitter-links-for-k12-institutions-processed.csv')
    ALL_USERNAMES = pd.unique(df['link'].map(extract_username_from_url))
    downloaded_usernames = list(map(lambda f: f.split('/')[1].split('_2021')[0], glob.glob('data/*.csv')))
    ALL_USERNAMES = [u for u in ALL_USERNAMES if u not in downloaded_usernames and u != '']
    
    print(f'Downloading {len(ALL_USERNAMES)} users...')
    for user in ALL_USERNAMES:
        print(f'\n### Downloading {user} ...###\n')
        download_and_save_account_tweets(token_file_path='bearer_token.txt', 
                                         user_name=user, verbose=True, 
                                         save_file=True)    