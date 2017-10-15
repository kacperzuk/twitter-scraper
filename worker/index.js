const { Client } = require('pg')
const Twitter = require('twitter')

const pg = {
    client: new Client(),
    connected: false,
    maybe_connect: async () => {
        if (!pg.connected)
            await pg.client.connect()
        pg.connected = true
    },
    fetch_from_queue: async () => {
        await pg.maybe_connect()
        const res = await pg.client.query(`
            update cmd_queue
            set is_processing_since = current_timestamp
            where id = (
                select id
                from cmd_queue
                where is_processing_since is null
                order by id
                for update skip locked
                limit 1
            )
            returning id, method, path, params, tag
        `)
        if (res.rows.length)
            return res.rows[0]
        return null
    },
    finish_cmd: async (cmd, result) => {
        await pg.maybe_connect()
        await pg.client.query('begin')
        try {
            await pg.client.query(`insert into res_queue (tag, result) values ($1, $2)`, [cmd.tag, result])
            await pg.client.query('delete from cmd_queue where id = $1', [cmd.id]);
            await pg.client.query('commit')
        } catch(e) {
            await pg.client.query('rollback')
            throw e
        }
    }
}

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
    const cmd = await pg.fetch_from_queue()
    if(cmd) {
        const tag = `Processed command ${JSON.stringify(cmd)}`
        console.time(tag)
        const result = await t[cmd.method](cmd.path, cmd.params);
        await pg.finish_cmd(cmd, result).catch(e => console.error(e.stack))
        console.timeEnd(tag)
    }
    setTimeout(run, 1000)
}

run()
