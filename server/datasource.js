const { DataSource } = require('apollo-datasource');
const ssb = require("./lib/ssb-client");
const pull = require("pull-stream");

const streamPull = (...streams) =>
  new Promise((resolve, reject) => {
    pull(
      ...streams,
      pull.collect((err, msgs) => {
        if (err) return reject(err);
        return resolve(msgs);
      })
    );
});

const network = "rinkeby";
const version = 1.0;

class ssbFlumeAPI extends DataSource {
  constructor() {
    super();
    this.ssb = ssb;
    this.network = network;
    this.version = version;
  }

  initialize(config) {
    this.context = config.context;
  }

  async promiseReducer(promise) {
      promise.value.content.from.reserves.type = "RESERVES";
      if (promise.value.content.from.commonName != null) {
        promise.value.content.from.commonName.type = "COMMON";
      }
      if (promise.value.content.to.reserves != null) {
        promise.value.content.to.reserves.type = "RESERVES";
      }
      if (promise.value.content.to.commonName != null) {
        promise.value.content.to.commonName.type = "COMMON";
      }
      if (promise.value.content.to.verifiedAccounts != null) {
        for (let j=0; j<promise.value.content.to.verifiedAccounts.length; j++) {
            promise.value.content.to.verifiedAccounts[j].type = "ACCOUNT";
        }
      }
      promise.value.content.to.type = "FEED";
      let reciprocity;
      if (promise.value.content.reciprocityId!=null) {
        reciprocity = await this.getCompleteReciprocityByIds({reciprocityId: promise.value.content.reciprocityId, feedId: promise.value.author});
      }
      return {
        type: "PROMISE",
        id: promise.key,
        sequence: promise.value.sequence,
        hash: promise.value.hash.toUpperCase(),
        previous: promise.value.previous,
        signature: promise.value.signature,
        timestamp: promise.value.timestamp,
        header: promise.value.content.header,
        amount: promise.value.content.promise.amount,
        denomination: promise.value.content.promise.denomination,
        memo: promise.value.content.promise.memo,
        tags: promise.value.content.promise.tags,
        author: {type: "FEED", id: promise.value.author, commonName: promise.value.content.from.commonName, reserves: promise.value.content.from.reserves},
        recipient: promise.value.content.to,
        nonce: promise.value.content.promise.nonce,
        issueDate: promise.value.content.promise.issueDate,
        vestDate: promise.value.content.promise.vestDate,
        claimName: promise.value.content.promise.claimName,
        claim: {
           data: promise.value.content.promise.claimData,
           fromSignature: promise.value.content.promise.fromSignature,
           toSignature: promise.value.content.promise.toSignature,
        },
        reciprocity: reciprocity,
      }
  }

  async identityReducer(idMsg) {
    idMsg.value.content.feed.type = "FEED";
    return {
      type: "IDENTITY",
      id: idMsg.key,
      sequence: idMsg.value.sequence,
      hash: idMsg.value.hash.toUpperCase(),
      header: idMsg.value.content.header,
      previous: idMsg.value.previous,
      signature: idMsg.value.signature,
      timestamp: idMsg.value.timestamp,
      author: {id: idMsg.value.author},
      feed: idMsg.value.content.feed,
      name: idMsg.value.content.name,
      evidence: idMsg.value.content.evidence,
    }
  }

  async completeSettlementReducer(msg) {
    return {
        type: "COMPLETE_SETTLEMENT",
        id: msg.key,
        author: {id: msg.value.author},
        hash: msg.value.hash.toUpperCase(),
        header: msg.value.content.header,
        previous: msg.value.previous,
        signature: msg.value.signature,
        timestamp: msg.value.timestamp,
        claim: msg.value.content.claim,
        claimName: msg.value.content.claimName,
        nonce: msg.value.content.nonce,
        amount: msg.value.content.amount,
        denomination: msg.value.content.denomination,
        tx: msg.value.content.tx,
    }
  }

  async proposeReciprocityReducer(msg) {
      return {
        type: "PROPOSE_RECIPROCITY",
        id: msg.key,
        author: {id: msg.value.author},
        hash: msg.value.hash.toUpperCase(),
        header: msg.value.content.header,
        previous: msg.value.previous,
        signature: msg.value.signature,
        timestamp: msg.value.timestamp,
        amount: msg.value.content.amount,
        denomination: msg.value.content.denomination,
        reciprocityId: msg.value.content.reciprocityId,
      }
  }

  async acceptReciprocityReducer(msg) {
    let proposal = await this.getProposeReciprocityById({reciprocityId: msg.value.content.reciprocityId});
    return {
        type: "ACCEPT_RECIPROCITY",
        id: msg.key,
        author: {id: msg.value.author},
        hash: msg.value.hash.toUpperCase(),
        header: msg.value.content.header,
        previous: msg.value.previous,
        signature: msg.value.signature,
        timestamp: msg.value.timestamp,
        proposal: proposal,
        incomingOriginalClaim: msg.value.content.incomingOriginalClaim,
        outgoingOriginalClaim: msg.value.content.outgoingOriginalClaim,
        incomingUpdatedClaim: msg.value.content.incomingUpdatedClaim,
        outgoingUpdatedClaim: msg.value.content.outgoingUpdatedClaim,
    }
  }

  async completeReciprocityReducer(msg) {
      let proposal = await this.getProposeReciprocityById({reciprocityId: msg.value.content.reciprocityId});
      return {
        type: "COMPLETE_RECIPROCITY",
        id: msg.key,
        author: {id: msg.value.author},
        hash: msg.value.hash.toUpperCase(),
        header: msg.value.content.header,
        previous: msg.value.previous,
        signature: msg.value.signature,
        timestamp: msg.value.timestamp,
        proposal: proposal,
        originalClaims: msg.value.content.originalClaims,
        updatedClaims: msg.value.content.updatedClaims
      }
  }

  async genericReducer(msg) {
      return {
        type: "GENERIC",
        id: msg.key,
        author: {id: msg.value.author},
        sequence: msg.value.sequence,
        hash: msg.value.hash.toUpperCase(),
        previous: msg.value.previous,
        signature: msg.value.signature,
        timestamp: msg.value.timestamp,
        content: JSON.stringify(msg.value.content),
    }
  }

  async getFeed({ feedId }) {
      let reserves, commonName;
      let cseq = -1;
      let rseq;
      let idMsgs = await this.getIdMsgsByFeedId({ feedId });
      let accounts = [];
      // Not the most efficient to search through all of the Id Messages from pub, every time
      let evidenceMsgs = await this.getIdMsgsByFeedId({feedId: ssb.client().id});
      let evidenceMap = {};
      for (let k=0; k<evidenceMsgs.length; k++) {
        if (evidenceMsgs[k].feed.id==feedId) {
            evidenceMap[evidenceMsgs[k].id] = evidenceMsgs[k];
        }
      }
      for (let j=0; j<idMsgs.length; j++) {
        if (idMsgs[j].feed.id == feedId) {
            if (idMsgs[j].name.type == "COMMON" && cseq<idMsgs[j].sequence) {
                commonName = idMsgs[j].name;
                cseq = idMsgs[j].sequence;
            }
            if (idMsgs[j].name.type == "RESERVES" && (reserves==null || rseq>idMsgs[j].sequence)) {
                reserves = idMsgs[j].name;
                rseq = idMsgs[j].sequence;
            }
            if (idMsgs[j].name.type == "ACCOUNT") {
                eMsg = evidenceMap[idMsgs[j].evidence.id];
                if (eMsg !== undefined) {
                    if (eMsg.name.type=="ACCOUNT" && eMsg.name.handle==idMsgs[j].name.handle && eMsg.name.accountType==idMsgs[j].name.accountType) {
                        accounts.push(idMsgs[j].name);
                    }
                    break
                }
            }
        }
      }
      let allPromises = await this.getAllPromises();
      let promised = [];
      let settlements = [];
      let settled = [];
      let accountStubs = [];
      for (let z=0; z<accounts.length; z++) {
          accountStubs.push(accounts[z].accountType+accounts[z].handle);
      }
      for (let i=0; i<allPromises.length; i++) {
          if (allPromises[i].isLatest && allPromises[i].recipient.id == feedId) {
            let settlement = await this.getSettlementByClaimName({ claimName: allPromises[i].claimName});
            if (settlement.length==0) {
                promised.push(allPromises[i]);
            } else {
                settlements.push(settlement[0]);
                settled.push(allPromises[i]);
            }
          } else if (allPromises[i].isLatest && allPromises[i].recipient.id==null) {             
            if (accountStubs.includes(allPromises[i].recipient.verifiedAccounts[0].accountType+allPromises[i].recipient.verifiedAccounts[0].handle)) {
                promised.push(allPromises[i]);
            }
          }
      }
      let promises = await this.getPromisesByFeedId({ feedId });
      let liabilities = [];
      for (let n=0; n<promises.length; n++) {
          if (promises[n].isLatest) {
            let settlement = await this.getSettlementByClaimName({ claimName: promises[n].claimName});
            if (settlement.length==0) {
                liabilities.push(promises[n]);
            } else {
                settlements.push(settlement[0]);
                settled.push(promises[n]);
            }
          }
      }
      let feedMsgs = await this.getFeedMessages({ feedId });
      return {
          type: "FEED",
          id: feedId,
          publicKey: feedId.substring(1, feedId.length),
          messages: feedMsgs,
          assets: promised,
          verifiedAccounts: accounts,
          reserves: reserves,
          commonName: commonName,
          liabilities: liabilities,
          settlements: settlements,
          settledPromises: settled,
      }
  }

  async getIdMsgsByFeedId({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                type: "cashless/identity",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        return Promise.all(results.map(async (result) => await this.identityReducer(result)));
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }  
  }

  async getAllIdMsgs() {
    const myQuery = [{
        "$filter": {
        value: {
            content: {
                type: "cashless/identity",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        return Promise.all(results.map(async (result) => await this.identityReducer(result)));
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getAllSettlementMsgs() {
    const myQuery = [{
        "$filter": {
        value: {
            content: {
                type: "cashless/complete-settlement",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        return Promise.all(results.map(async (result) => await this.completeSettlementReducer(result)));
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    } 
  }
  async getSettlementByClaimName({ claimName }) {
    const myQuery = [{
        "$filter": {
        value: {
            content: {
                type: "cashless/complete-settlement",
                header: {version: this.version, network: this.network},
                claimName: claimName,
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        return Promise.all(results.map(async (result) => await this.completeSettlementReducer(result)));
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    } 
  }
  async getPendingPromisesByFeedId({ feedId }) {
    let promises = await this.getPromisesByFeedId({ feedId });
    let output = [];
    for (let i=0; i<promises.length; i++) {
        if (promises[i].isLatest && promises[i].nonce==0) {
            output.push(promises[i]);
        }
    }
    return output;
  }

  async getPromisesByFeedId({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                type: "cashless/promise",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let promises = Promise.all(results.map(async (result) => await this.promiseReducer(result)));
        let promiseMap = {};
        for (let j=0; j<promises.length; j++) {
            promises[j].isLatest = false;
            if (promiseMap[promises[j].claimName] !== undefined) {
                promiseMap[promises[j].claimName].push(promises[j]);
            } else {
                promiseMap[promises[j].claimName] = [promises[j]];
            }
        }
        let keys = Object.keys(promiseMap);
        let finalPromises = [];
        for (let i=0; i<keys.length; i++) {
            let pList = promiseMap[keys[i]];
            let proms = pList.sort((a, b) => (a.nonce > b.nonce) ? -1 : 1);
            proms[0].isLatest = true;
            finalPromises.push(...proms);
        }
        return finalPromises;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getAllPromises() {
    try {
        let ids = await this.getFeedIds();
        let promises = [];
        for (let i=0; i<ids.length; i++) {
            let feedPromises = await this.getPromisesByFeedId({feedId: ids[i]});
            promises.push(...feedPromises);
        }
        return promises;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getActivePromises() {
    let allPromises = await this.getAllPromises();
    let promises = [];
    for (let i=0; i<allPromises.length; i++) {
        if (allPromises[i].isLatest) {
            if (allPromises[i].nonce > 0) {
                let settlement = await this.getSettlementByClaimName({ claimName: allPromises[i].claimName});
                if (settlement.length == 0) {
                    promises.push(allPromises[i]);
                }
            } else {
                promises.push(allPromises[i]);
            }
        }
    }

    return promises;
  }

  async getPromiseChain({ claimName, feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                header:  {version: this.version, network: this.network},
                promise: {
                    claimName: claimName
                }
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let promises = Promise.all(results.map(async (result) => await this.promiseReducer(result)));
        let highest = 0;
        for (let i=0; i<promises.length; i++) {
            if (promises[i].nonce > highest) {
                highest = promises[i].nonce;
            }
        }
        for (let j=0; j<promises.length; j++) {
            if (promises[j].nonce<highest) {
                promises[j].isLatest = false;
            } else {
                promises[j].isLatest = true;
            }
        }
        return promises;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getPromise({ claimName, feedId, nonce }) {
    let promiseChain = await this.getPromiseChain({claimName, feedId});
    let promise;
    for (let i=0; i<promiseChain.length; i++) {
        if (promiseChain[i].nonce==nonce) {
            promise = promiseChain[i];
            break
        }
    }

    return promise;
  }

  async getProposeReciprocityById({ reciprocityId }) {
    let proposals = await this.getReciprocityProposalsByFeedId({feedId: ssb.client().id});
    let proposal;
    for (let i=0; i<proposals.length; i++) {
        if (proposals[i].reciprocityId == reciprocityId) {
            proposal = proposals[i];
            break
        }
    }

    return proposal;
  }

  async getActiveReciprocityProposalsByFeedId({ feedId }) {
    let proposals = await this.getReciprocityProposalsByFeedId({feedId: feedId});
    let completed = await this.getAllCompletedReciprocityIds();
    let activeProposals = [];
    for (let i=0; i<proposals.length; i++) {
        if (!completed.includes(proposals[i].reciprocityId)) {
            activeProposals.push(proposals);
        }
    }

    return proposals;
  }

  async getReciprocityProposalsByFeedId({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                type: "cashless/propose-reciprocity",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let proposals = Promise.all(results.map(async (result) => await this.proposeReciprocityReducer(result)));
        for (let i=0; i<results.length; i++) {
            let promises = [];
            let promiseRefs = results[i].promises;
            for (let j=0; j<promiseRefs.length; j++) {
                let promise = await this.getPromise(promiseRefs[j]);
                promises.push(promise);
            }
            proposals[i].promises = promises;
        }

        return proposals;
    } catch (_e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getReciprocityAcceptMsgsByFeedId({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                type: "cashless/accept-reciprocity",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let acceptMsgs = Promise.all(results.map(async (result) => await this.acceptReciprocityReducer(result)));
        return acceptMsgs;
    } catch (_e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getReciprocityAcceptMsgsByReciprocityId({ reciprocityId }) {
    const myQuery = [{
        "$filter": {
        value: {
            content: {
                type: "cashless/accept-reciprocity",
                header: {version: this.version, network: this.network},
                reciprocityId: reciprocityId,
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let acceptMsgs = Promise.all(results.map(async (result) => await this.acceptReciprocityReducer(result)));
        return acceptMsgs;
    } catch (_e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getAllReciprocityAcceptMsgs() {
    try {
        let ids = await this.getFeedIds();
        let acceptMsgs = [];
        for (let i=0; i<ids.length; i++) {
            let accepts = await this.getReciprocityAcceptMsgsByFeedId({feedId: ids[i]});
            acceptMsgs.push(...accepts);
        }
        return acceptMsgs;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getAllCompletedReciprocityIds() {
      let rmsgs = await this.getAllCompletedReciprocityMsgs();
      let set = new Set(rmsgs.map(r => r.reciprocityId))
      return [...set];
  }

  async getCompletedReciprocityMsgsByFeedId({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId,
            content: {
                type: "cashless/complete-reciprocity",
                header: {version: this.version, network: this.network},
            }
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let crMsgs = Promise.all(results.map(async (result) => await this.completeReciprocityReducer(result)));
        return crMsgs;
    } catch (_e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getAllCompletedReciprocityMsgs() {
    try {
        let ids = await this.getFeedIds();
        let crMsgs = [];
        for (let i=0; i<ids.length; i++) {
            let feedCRs = await this.getCompletedReciprocityMsgsByFeedId({feedId: ids[i]});
            crMsgs.push(...feedCRs);
        }
        return crMsgs;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }

  async getFeedIds() {
      let msgs = await this.getAllIdMsgs();
      let allIds = new Set(msgs.map(msg => msg.author.id));
      return [...allIds];
  }

  async getFeedMessages({ feedId }) {
    const myQuery = [{
        "$filter": {
        value: {
            author: feedId
        }
      }
    }];
    try {
        let results = await streamPull(
            this.ssb.client().query.read({
                query: myQuery,
            })
        );
        let promises = await this.getPromisesByFeedId({ feedId });
        let promiseDict = {};
        for (let j=0; j<promises.length; j++) {
            promiseDict[promises[j].id] = promises[j];
        }
        let output = [];
        for (let i=0; i<results.length; i++) {
            if (results[i].value.content.type=="cashless/promise") {
                output.push(promiseDict[results[i].key]);
            } else if (results[i].value.content.type=="cashless/identity") {
                let idMsg = await this.identityReducer(results[i]);
                output.push(idMsg);
            } else if (results[i].value.content.type=="cashless/complete-settlement") {
                let completeSettlementMsg = await this.completeSettlementReducer(results[i]);
                output.push(completeSettlementMsg);
            } else if (results[i].value.content.type=="cashless/propose-reciprocity") {
                let proposeReciprocityMsg = await this.proposeReciprocityReducer(results[i]);
                output.push(proposeReciprocityMsg);
            } else if (results[i].value.content.type=="cashless/complete-reciprocity") {
                let completeReciprocityMsg = await this.completeReciprocityReducer(results[i]);
                output.push(completeReciprocityMsg);
            } else {
                let genericMsg = await this.genericReducer(results[i]);
                output.push(genericMsg);
            }
        }
        return output;
    } catch(e) {
        console.log("ERROR QUERYING FLUME DB:", e);
        return [];
    }
  }
}

module.exports = ssbFlumeAPI;