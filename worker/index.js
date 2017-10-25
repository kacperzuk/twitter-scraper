const { Client } = require('pg')
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

const ratelimited = {};

function getPG() {
    const pg = {
        client: new Client(),
        connected: false,
        maybe_connect: async () => {
            if (!pg.connected) {
                await pg.client.connect()
            }
            pg.connected = true
        },
        fetch_from_queue: async () => {
            await pg.maybe_connect()
            const placeholders = Object.keys(ratelimited).map((v,i) => `$${i+1}`).join(", ");
            const tagfilter = placeholders ? `and tag not in (${placeholders})` : "";
            const res = await pg.client.query(`
                update cmd_queue
                set
                    is_processing_since = current_timestamp,
                    tries = tries + 1
                where id = (
                    select id
                    from cmd_queue
                    where is_processing_since is null
                        ${tagfilter}
                    order by id
                    for update skip locked
                    limit 1
                )
                returning id, method, path, params, tag, metadata
            `, Object.keys(ratelimited))
            if (res.rows.length)
                return res.rows[0]
            return null
        },
        cancel_cmd: async (cmd) => {
            await pg.maybe_connect()
            await pg.client.query('update cmd_queue set is_processing_since = null where id = $1', [cmd.id]);
        },
        finish_cmd: async (cmd, result) => {
            await pg.maybe_connect()
            await pg.client.query('begin')
            try {
                await pg.client.query(`insert into res_queue (tag, result, metadata) values ($1, $2, $3)`, [cmd.tag, JSON.stringify(result), cmd.metadata])
                await pg.client.query('delete from cmd_queue where id = $1', [cmd.id]);
                await pg.client.query('commit')
            } catch(e) {
                await pg.client.query('rollback')
                await pg.cancel_cmd(cmd)
                throw e
            }
        }
    }
    return pg;
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

const run = async (pg, t) => {
    if(!pg) {
        pg = getPG();
    }
    if(!t) {
        t = gett();
    }
    const cmd = await pg.fetch_from_queue()
    let error;
    if(cmd) {
        cmd.params = JSON.parse(cmd.params)
        const result = await t[cmd.method](cmd.path, cmd.params).catch((err) => {
            console.log(new Date(), "Failure!")
            console.log("cmd: ", cmd)
            console.log(err)
            console.log(err.name)
            error = err
        });
        if(result) {
            await pg.finish_cmd(cmd, result).catch(e => console.error(e.stack))
            console.log(new Date(), `Processed command ${JSON.stringify(cmd).substr(0, 140)}`)
        }
        if(error && error.some) {
            if(error.some(e => e.code == 88)) {
                console.warn(new Date(), "Got rate limit error, ignoring tag "+cmd.tag+" for 1 minute...")
                await pg.cancel_cmd(cmd)
                ratelimited[cmd.tag] = true;
                setTimeout(() => {
                    if (cmd.tag in ratelimited) {
                        delete ratelimited[cmd.tag];
                    }
                }, 60*1000)
            } else if(error.some(e => e.code == 34)) {
                await pg.finish_cmd(cmd, error[0]);
            } else {
                await pg.cancel_cmd(cmd)
            }
        } else if (error && error.name == "Error") {
            await pg.finish_cmd(cmd, { error: "unauthorized" });
        }
        setImmediate(run.bind(null, pg, t))
    } else {
        setTimeout(run.bind(null, pg, t), 100)
    }
}

run()
run()
run()
