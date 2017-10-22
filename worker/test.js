const Twitter = require('twitter')

const uid = "749131601905352700";

const t = {
    client: new Twitter({
      consumer_key: process.env.TWITTER_CONSUMER_KEY,
      consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
      access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
      access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
    }),
    get: async (path, params) => {
        return await t.client.get(path, params);
    },
    post: async (path, params) => {
        return await t.client.post(path, params);
    }
};

const run = async () => {
    console.log(await t.get("users/lookup", { user_id: uid }).catch(err => console.log(err)));
}

run()
