//#region global variable declarations
const express = require ('express');
const app = express();
const http = require('http');
const fs = require('fs')
const path = require('path');
const server = http.createServer(app);
const {Server} = require ("socket.io");
const io = new Server(server);
const platformclient =  require ('purecloud-platform-client-v2');
const apiclient = platformclient.ApiClient.instance;
const cryptomodule = require ('crypto')
const localtunnel = require ('localtunnel');
const readline = require('readline');
const { program } = require('commander');
const { json } = require('stream/consumers');
require('dotenv').config();


const openmessagingintegrationid = process.env.INTEGRATION_ID;


var conversationapi;
var senderid = cryptomodule.randomUUID();
var connectiondictionary = {};
var convid = false;
var conversationid;
var messagingid;
var debug = false;
var tunnel = null;
var simulatefailures =  false;
var localtunnelused = false;
var currenturl = "https://cute-lions-act.loca.lt/openmessagingwebhook";
var publicurldomain = "httsp://example.com";
var rejectcode = 403;

//#endregion

//#region web server handling
app.use(express.json());


app.get('/', (req,res) => {
    res.sendFile(__dirname + '/index.html');
});

app.get('/public/:filename',(req,res) => {

    res.sendFile(__dirname + "/public/" + req.params.filename);
})

app.post('/openmessagingwebhook', (req,res) => {

   // Logger("RECV","new webhook message received...");

    if (debug){
        Logger("RECV", "Webhook message headers " + JSON.stringify(req.headers, null, 2));
    }

 
    // integration - the integration object
    // normalizedMessage - the NormalizedMessage payload
    // request - webhook request object

    const jsonrequestbody = JSON.stringify(req.body);
    const signature = req.headers['x-hub-signature-256'];
    const messageHash = cryptomodule.createHmac('sha256',  process.env.INTEGRATION_SECRET)
     .update(jsonrequestbody)
     .digest('base64');
   
    if (`sha256=${messageHash}` !== signature) {
         //throw new Error("Webhook Validation Failed! Throw this away.");
         Logger("ERROR","X-HUB-Signature-256 validation failed, rejecting request.");
         res.sendStatus(403).end();
    }
    else if (simulatefailures) {
        Logger("RECV","Webhook message " + JSON.stringify(req.body, null, 2));
        Logger("INFO", "Rejecting request with 403");
        res.sendStatus(rejectcode);
    }
    else {
        res.sendStatus(200);
        var jsondata = JSON.parse(jsonrequestbody);

        if (jsondata.type == 'Text')
        {
            Logger("RECV","Outbound text message " + JSON.stringify(req.body, null, 2));
            if (!messagingid)
            {
                 messagingid =  jsondata.id;
            }

            if(!convid)
            {
                var msgid = jsondata.channel.messageId;
            
                let opts = { 
                     'useNormalizedMessage': false // Boolean | If true, response removes deprecated fields (textBody, media, stickers)
                };

                conversationapi.getConversationsMessageDetails(msgid, opts)
                .then((data) => {
                   // console.log(`getConversationsMessageDetails success! data: ${JSON.stringify(data, null, 2)}`);
                    convid = true;
                    conversationid = data.conversationId;
                   Logger("INFO","Associated conversation ID: " + conversationid);
                })
                .catch((err) => {
                    Logger("ERROR","Failure retrieving conversation ID " +  err);

                });
            }
            if(jsondata.content)
            {
                io.sockets.emit('chat message attachment',jsondata.channel.from.nickname, jsondata.content[0].attachment.filename, jsondata.content[0].attachment.url);
            }
            else{
                 io.sockets.emit('chat message',jsondata.channel.from.nickname,jsondata.text);
            }
            SendReceiptToOrg(jsondata);
    
        }
        else if (jsondata.type == 'Event')
        {
            if (jsondata.events[0].eventType == 'Typing')
            {
                Logger("RECV","Outbound Typing Event " + JSON.stringify(req.body, null, 2));
                io.sockets.emit('typingind','');                            
            }
        }
        else if (jsondata.type == "Receipt")
        {
            Logger("RECV","Receipt for inbound message " + JSON.stringify(req.body, null, 2));
        }
    }
});

//#endregion

//#region webpage event handling

io.on('connection', (socket) => {
        
   // connectiondictionary[senderid] = socket.id; 
   
   // web page user sent chat message, forwarding it to Genesys Cloud
    socket.on('new_message_from_page', (sender, customparticipantdata, msg) => {
    const now = new Date();
    var body = {
        "id":senderid,
        "channel": {
            "messageId":cryptomodule.randomUUID(),
            "from":{
                "nickname":sender.fromname,
                "id":sender.fromaddress,
                "idType":"email"
            },
            "metadata": {
                "customAttributes": {
                    "demokey":customparticipantdata
                }
            },
            "time":now.toISOString()
        },
        "text":msg,
        "direction":"Inbound"
    };

    if (customparticipantdata == ""){
        body = {
            "id":senderid,
            "channel": {
                "messageId":cryptomodule.randomUUID(),
                "from":{
                    "nickname":sender.fromname,
                    "id":sender.fromaddress,
                    "idType":"email"
                },
                "time":now.toISOString()
            },
            "text":msg,
            "direction":"Inbound"
        };
    }

       conversationapi.postConversationsMessageInboundOpenMessage(openmessagingintegrationid, body)
        .then((data) => {
            Logger("XMIT","Sent inbound message " + JSON.stringify(data, null, 2));
        })
        .catch((err) => {
            Logger("ERROR", "Failure sending inbound message " + err);
        });

    });

    //web page user added attachment file, saving it to server, sending to Genesys Cloud and informing web page of saved location to display in feed
    socket.on('new_file',(sender, filename, filedata) => {
       
        const fileName = filename; 
        const savePath = path.join(__dirname, 'public', fileName);
        
        try {

            fs.writeFileSync(savePath, filedata);

            const now = new Date();
            var body = {
                    "id":senderid,
                    "channel": {
                        "messageId":cryptomodule.randomUUID(),
                        "from":{
                            "nickname":sender.fromname,
                            "id":sender.fromaddress,
                            "idType":"email"
                        },
                        "time":now.toISOString()
                    },
                    "direction":"Inbound",
                    'text':fileName,
                    "content": [{
                        "attachment": {
                            "mediaType": "Image",
                            "url":  publicurldomain + "/public/" + fileName,
                            "mime": "image/jpeg",
                            "filename": fileName
                        }
                    }]
                };

                conversationapi.postConversationsMessageInboundOpenMessage(openmessagingintegrationid, body)
                    .then((data) => {
                    Logger("XMIT","Sent inbound message " + JSON.stringify(data, null, 2));
                })
                .catch((err) => {
                    Logger("ERROR", "Failure sending attachment message " + err);
                });
        
                io.sockets.emit('confirm_file_upload_complete',fileName, publicurldomain + "/public/" + fileName);
        }
        catch{
            Logger("ERROR", "Failure saving attachment to server");
        }  
    })

    // web page user typing in input box, sending event to Genesys Cloud
    socket.on('page_visitor_typing', (msg) => {

        const now = new Date();
        var body = {
               "channel": {
                    "from":{
                        "nickname":msg.fromname,
                        "id":msg.fromaddress,
                        "idType":"email"
                    },
                    "time":now.toISOString()
               },
               "events":[
                {
                    "eventType":"Typing"
                }
               ]
           };
          
        conversationapi.postConversationsMessageInboundOpenEvent(openmessagingintegrationid, body)
        .then((data) => {
            Logger("XMIT", "Sent inbound typing event " + JSON.stringify(data, null, 2));
         })
        .catch((err) => {
             Logger("ERROR","Failure sending inbound typing event " + err);
        });
    
    });

    //web user closed browser, disconnecting conversation on Genesys Cloud
    socket.on('disconnect', () => {
        Logger("INFO","Webpage user disconnected.");

        if(conversationid){
            Logger("INFO","Disconnecting conversation: " + conversationid);
           
            let body = {"state":"disconnected"};

            conversationapi.patchConversationsMessage(conversationid, body)
                .then((data) => {
                   Logger("INFO","patchConversationsMessage success! data: " + JSON.stringify(data, null, 2));
                })
                .catch((err) => {
                    Logger("ERROR","There was a failure disconnecting conversation" + err);
            });
        }

    });
});

//#endregion

//#region console key input handling


if (process.stdin.isTTY) {
    process.stdin.on("keypress", (str, key) => {
        switch (str) {
            case "q":
                CloseServer();
                break;
        }
    });
}

//#endregion

//#region function definitions
function Update_Integration_Webhook_URL() {
    // Render / other production hosting
    if (process.env.PUBLIC_URL) {
        publicurldomain = process.env.PUBLIC_URL.replace(/\/+$/, "");
    }
    // Local development using localtunnel
    else if (localtunnelused && tunnel && tunnel.url) {
        publicurldomain = tunnel.url.replace(/\/+$/, "");
    }
    // GitHub Codespaces
    else if (
        process.env.CODESPACE_NAME &&
        process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN
    ) {
        publicurldomain =
            "https://" +
            process.env.CODESPACE_NAME +
            "-3000." +
            process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN;
    }
    else {
        Logger(
            "ERROR",
            "Cannot determine public URL. Set PUBLIC_URL environment variable."
        );
        return;
    }

    currenturl = publicurldomain + "/openmessagingwebhook";

    const body = {
        outboundNotificationWebhookUrl: currenturl
    };

    Logger("INFO", "Updating Genesys webhook to " + currenturl);

    conversationapi
        .patchConversationsMessagingIntegrationsOpenIntegrationId(
            openmessagingintegrationid,
            body
        )
        .then((data) => {
            Logger(
                "INFO",
                "Updated Integration Webhook URL to " +
                    data.outboundNotificationWebhookUrl
            );
        })
        .catch((err) => {
            Logger(
                "ERROR",
                "Failed to update Integration Webhook URL: " + err
            );
        });
}
function CloseServer() {
    Logger("END", "Shutting down web server...");

    if (tunnel && typeof tunnel.close === "function") {
        tunnel.close();
    }

    if (typeof server.closeAllConnections === "function") {
        server.closeAllConnections();
    }

    server.close(() => {
        Logger("END", "Web server closed");
        process.exit(0);
    });

    setTimeout(() => {
        process.exit(0);
    }, 5000).unref();
}

function Logger(state, data){
    console.log(new Date().toISOString() + " - " + state + " -- " + data );
}

function SendReceiptToOrg(recvdmessagejson) {
    
    const now = new Date();
    var body = {
            "id":messagingid,
            "channel": {
                "to": {
                    "id":recvdmessagejson.channel.id,
                    "idType":"email"
                },
            "time":now.toISOString()
            },
            "status":"Delivered",
            "direction":"Outbound",
            "isFinalReceipt":true
        };
    
  conversationapi.postConversationsMessageInboundOpenReceipt(openmessagingintegrationid,body)
  .then((data) => {
        Logger("XMIT","Sent receipt for outbound message" + JSON.stringify(data, null, 2));
  })
  .catch((err) => {
        Logger("ERROR", "Failure sending receipt for outbound message " + err);
  });
}

async function main() {
    /*
     * ---------------------------------------------------------
     * Interactive console
     * ---------------------------------------------------------
     *
     * process.stdin.setRawMode() exists when Node is attached
     * to a TTY. Render does not provide one.
     */
    if (
        process.stdin.isTTY &&
        typeof process.stdin.setRawMode === "function"
    ) {
        process.stdin.setRawMode(true);
        readline.emitKeypressEvents(process.stdin);

        Logger("INFO", "Interactive console enabled");
    } else {
        Logger(
            "INFO",
            "No interactive terminal detected; keyboard controls disabled"
        );
    }

    /*
     * ---------------------------------------------------------
     * Genesys Cloud region
     * ---------------------------------------------------------
     */
    switch (process.env.AWS_REGION) {
        case "eu-west-1":
            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.eu_west_1
            );
            break;

        case "eu-west-2":
            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.eu_west_2
            );
            break;

        case "eu-central-1":
            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.eu_central_1
            );
            break;

        case "eu-central-2":
            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.eu_central_2
            );
            break;

        case "us-west-2":
            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.us_west_2
            );
            break;

        default:
            Logger(
                "INFO",
                "Unknown/missing AWS_REGION; defaulting to us-east-1"
            );

            apiclient.setEnvironment(
                platformclient.PureCloudRegionHosts.us_east_1
            );
            break;
    }

    /*
     * ---------------------------------------------------------
     * Validate required configuration
     * ---------------------------------------------------------
     */
    const requiredVariables = [
        "OAUTH_ID",
        "OAUTH_PW",
        "INTEGRATION_ID",
        "INTEGRATION_SECRET"
    ];

    const missingVariables = requiredVariables.filter(
        (name) => !process.env[name]
    );

    if (missingVariables.length > 0) {
        Logger(
            "ERROR",
            "Missing required environment variables: " +
                missingVariables.join(", ")
        );

        process.exit(1);
    }

    /*
     * ---------------------------------------------------------
     * Start web server
     * ---------------------------------------------------------
     *
     * Render supplies PORT automatically.
     */
    const PORT = Number(process.env.PORT) || 3000;

    server.listen(PORT, "0.0.0.0", () => {
        Logger(
            "START",
            "Web server listening on 0.0.0.0:" + PORT
        );
    });

    /*
     * ---------------------------------------------------------
     * Genesys authentication
     * ---------------------------------------------------------
     */
    try {
        await apiclient.loginClientCredentialsGrant(
            process.env.OAUTH_ID,
            process.env.OAUTH_PW
        );

        Logger(
            "START",
            "Platform API initialized successfully"
        );

        conversationapi =
            new platformclient.ConversationsApi();

        /*
         * -----------------------------------------------------
         * Render / production
         * -----------------------------------------------------
         */
        if (process.env.PUBLIC_URL) {
            publicurldomain =
                process.env.PUBLIC_URL.replace(/\/+$/, "");

            Logger(
                "START",
                "Using configured public URL: " +
                    publicurldomain
            );

            Update_Integration_Webhook_URL();
            return;
        }

        /*
         * -----------------------------------------------------
         * GitHub Codespaces
         * -----------------------------------------------------
         */
        if (process.env.CODESPACES) {
            Logger(
                "START",
                "GitHub Codespaces detected"
            );

            Update_Integration_Webhook_URL();
            return;
        }

        /*
         * -----------------------------------------------------
         * Local development
         * -----------------------------------------------------
         */
        Logger(
            "START",
            "No PUBLIC_URL detected; starting LocalTunnel"
        );

        try {
            tunnel = await localtunnel({
                port: PORT
            });

            localtunnelused = true;

            Logger(
                "START",
                "Local tunnel open: " + tunnel.url
            );

            Update_Integration_Webhook_URL();

            tunnel.on("close", () => {
                Logger(
                    "INFO",
                    "LocalTunnel connection closed"
                );
            });
        } catch (tunnelError) {
            Logger(
                "ERROR",
                "Unable to start LocalTunnel: " +
                    tunnelError
            );
        }
    } catch (err) {
        Logger(
            "ERROR",
            "Platform API initialization failed: " + err
        );
    }
}

//#endregion

//#region command line parser
program
    .option('-h, --help', 'help')
    .option('-r, --rejectrequests','simulate failures')
    .option('-d, --debug','debug logging')
    .option('-v, --verbose','verbose logging');


program.parse(process.argv);
const options = program.opts();

if (options.help) {
    console.log("Usage: node server.js [options]");
    console.log("Options: ");
    console.log("   -h     displays this help");
    console.log("   -d     enables extended response for all API function calls");
    console.log("   -v     enabled verbose API logging with request and response body and header");
    console.log("   -r XXX enables webhook rejection with HTTP status code XXX");

}
else if (options.rejectrequests){
    simulatefailures = true;
    rejectcode = program.args[0];
    main();
}
else if (options.debug)
{
    debug = true;    
    apiclient.setReturnExtendedResponses(true);

    main();
}
else if (options.verbose)
{
    debug = true;
    apiclient.config.logger.log_level = apiclient.config.logger.logLevelEnum.level.LTrace;
    apiclient.config.logger.log_format = apiclient.config.logger.logFormatEnum.formats.TEXT;
    apiclient.config.logger.log_request_body = true;
    apiclient.config.logger.log_response_body = true;
    apiclient.config.logger.log_to_console = true;
    apiclient.config.logger.setLogger(); // To apply above changes

    main();
}
else 
{
    debug = false;   
    main();
}

//#endregion
