const Twitter = require('twitter')

function getBearerToken() {
    const oauth2 = new (require('oauth').OAuth2)(
        process.env.TWITTER_CONSUMER_KEY,
        process.env.TWITTER_CONSUMER_SECRET,
        'https://api.twitter.com/',
        null,
        'oauth2/token',
        null);
    return new Promise((resolve, reject) => {
        oauth2.getOAuthAccessToken( '', {'grant_type':'client_credentials'}, function (e, access_token, refresh_token, results) {
            if(e) reject(e)
            else resolve(access_token)
        });
    });
}

function gett() {
    const t = {
        init: async () => {
            t.client = new Twitter({
              consumer_key: process.env.TWITTER_CONSUMER_KEY,
              consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
              bearer_token: await getBearerToken()
            });
        },
        get: async (path, params) => {
            if (!t.client) await t.init()
            return await t.client.get(path, params);
        },
        post: async (path, params) => {
            if (!t.client) await t.init()
            return await t.client.post(path, params);
        }
    };
    return t;
}

const t = gett();
t.get("users/lookup", { screen_name: process.argv[2] }).catch((err) => {
    console.log(new Date(), "Failure!")
    console.log(err)
    console.log(err.name)
    error = err
}).then((resp) => {
    console.log(resp);
})
