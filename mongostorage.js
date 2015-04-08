/**
 * Copyright 2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

var mongo = require('mongodb');
var when = require('when');
var util = require('util');

var settings;

var mongodb;
var appname;

//var heartBeatLastSent = (new Date()).getTime();
//
//setInterval(function () {
//    var now = (new Date()).getTime();
//    if (mongodb && now - heartBeatLastSent > 15000) {
//        heartBeatLastSent = now;
//        mongodb.command({ ping: 1}, function (err, result) {});
//    }
//}, 15000);

function jconv(credentials) {
    var jconvs = {};
    for (id in credentials) {
        jconvs[id.replace("_", ".")] = credentials[id];
    }
    return jconvs;
}

function bconv(credentials) {
    var bconvs = {};
    for (id in credentials) {
        bconvs[id.replace(".", "_")] = credentials[id];
    }
    return bconvs;
}

function db() {
    return when.promise(function(resolve,reject,notify) {
        if (!mongodb) {
            mongo.MongoClient.connect(settings.mongoUrl,
                {
                    db:{
                        retryMiliSeconds:1000,
                        numberOfRetries:3
                    },
                    server:{
                        poolSize:1,
                        auto_reconnect:true,
                        socketOptions:{
                            socketTimeoutMS:10000,
                            keepAlive:1
                        }
                    }
                },
                function(err,_db) {
                    if (err) {
                        util.log("Mongo DB error:"+err);
                        reject(err);
                    } else {
                        mongodb = _db;
                        resolve(_db);
                    }
                }
            );
        } else {
            resolve(mongodb);
        }
    });
}

function collection() {
    return when.promise(function(resolve,reject,notify) {
        db().then(function(db) {
            db.collection(settings.mongoCollection||"nodered",function(err,_collection) {
                if (err) {
                    util.log("Mongo DB error:"+err);
                    reject(err);
                } else {
                    resolve(_collection);
                }
            });
        }).otherwise(function (err) {
            reject(err);
        })
    });
}

function libCollection() {
    return when.promise(function(resolve,reject,notify) {
        db().then(function(db) {
            db.collection(settings.mongoCollection||"nodered"+"-lib",function(err,_collection) {
                if (err) {
                    util.log("Mongo DB error:"+err);
                    reject(err);
                } else {
                    resolve(_collection);
                }
            });
        }).otherwise(function (err) {
            reject(err);
        })
    });
}

function close() {
    return when.promise(function(resolve,reject,notify) {
        if (mongodb) {
            mongodb.close(true, function(err,result) {
                if (err) {
                    util.log("Mongo DB error:"+err);
                    reject(err);
                } else {
                    resolve();
                }
            })
            mongodb = null;
        }
    });
}

function timeoutWrap(func) {
    return when.promise(function(resolve,reject,notify) {
        var promise = func().timeout(5000,"timeout");
        promise.then(function(a,b,c,d) {
            //heartBeatLastSent = (new Date()).getTime();
            resolve(a,b,c,d);
        });
        promise.otherwise(function(err) {
            console.log("TIMEOUT: ",func.name);
            if (err == "timeout") {
                close().then(function() {
                    resolve(func());
                }).otherwise(function(err) {
                    reject(err);
                });
            } else {
                reject(err);
            }
        });
    });
}

function getFlows() {
    var defer = when.defer();
    collection().then(function(collection) {
        collection.findOne({appname:appname},function(err,doc) {
            if (err) {
                defer.reject(err);
            } else {
                if (doc && doc.flow) {
                    defer.resolve(doc.flow);
                } else {
                    defer.resolve([]);
                }
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function saveFlows(flows) {
    console.log("saveFlows");
    var defer = when.defer();
    collection().then(function(collection) {
        collection.update({appname:appname},{$set:{appname:appname,flow:flows}},{upsert:true},function(err) {
            if (err) {
                defer.reject(err);
            } else {
                defer.resolve();
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function getCredentials() {
    var defer = when.defer();
    collection().then(function(collection) {
        collection.findOne({appname:appname},function(err,doc) {
            if (err) {
                defer.reject(err);
            } else {
                if (doc && doc.credentials) {
                    defer.resolve(jconv(doc.credentials));
                } else {
                    defer.resolve({});
                }
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function saveCredentials(credentials) {
    var defer = when.defer();
    collection().then(function(collection) {
        collection.update({appname:appname},{$set: {"credentials":bconv(credentials)}},{upsert:true},function(err) {
            if (err) {
                defer.reject(err);
            } else {
                defer.resolve();
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function getSettings () {
    var defer = when.defer();
    collection().then(function(collection) {
        collection.findOne({appname:appname},function(err,doc) {
            if (err) {
                defer.reject(err);
            } else {
                if (doc && doc.settings) {
                    defer.resolve(jconv(doc.settings));
                } else {
                    defer.resolve({});
                }
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function saveSettings (settings) {
    var defer = when.defer();
    collection().then(function(collection) {
        collection.update({appname:appname},{$set: {"settings":bconv(settings)}},{upsert:true},function(err) {
            if (err) {
                console.log(err);
                defer.reject(err);
            } else {
                defer.resolve();
            }
        })
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function getAllFlows() {
    var defer = when.defer();
    libCollection().then(function(libCollection) {
        libCollection.find({appname:appname, type:'flow'},{sort:'path'}).toArray(function(err,docs) {
            if (err) {
                defer.reject(err);
            } else if (!docs) {
                defer.resolve({});
            } else {
                var result = {};
                for (var i=0;i<docs.length;i++) {
                    var doc = docs[i];
                    var path = doc.path;
                    var parts = path.split("/");
                    var ref = result;
                    for (var j=0;j<parts.length-1;j++) {
                        ref['d'] = ref['d']||{};
                        ref['d'][parts[j]] = ref['d'][parts[j]]||{};
                        ref = ref['d'][parts[j]];
                    }
                    ref['f'] = ref['f']||[];
                    ref['f'].push(parts.slice(-1)[0]);
                }
                defer.resolve(result);
            }
        });
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function getFlow(fn) {
    var defer = when.defer();
    libCollection().then(function(libCollection) {
        libCollection.findOne({appname:appname, type:'flow', path:fn},function(err,doc) {
            if (err) {
                defer.reject(err);
            } else if (doc&& doc.data) {
                defer.resolve(doc.data);
            } else {
                defer.reject();
            }
        });
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function saveFlow(fn,data) {
    var defer = when.defer();
    libCollection().then(function(libCollection) {
        libCollection.update({appname:appname,type:'flow',path:fn},{appname:appname,type:'flow',path:fn,data:data},{upsert:true},function(err) {
            if (err) {
                defer.reject(err);
            } else {
                defer.resolve();
            }
        });
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function getLibraryEntry(type,path) {
    var defer = when.defer();
    libCollection().then(function(libCollection) {
        libCollection.findOne({appname:appname, type:type, path:path}, function(err,doc) {
            if (err) {
                defer.reject(err);
            } else if (doc) {
                defer.resolve(doc.data);
            } else {
                if (path != "" && path.substr(-1) != "/") {
                    path = path+"/";
                }
                libCollection.find({appname:appname, type:type, path:{$regex:path+".*"}},{sort:'path'}).toArray(function(err,docs) {
                    if (err) {
                        defer.reject(err);
                    } else if (!docs) {
                        defer.reject("not found");
                    } else {
                        var dirs = [];
                        var files = [];
                        for (var i=0;i<docs.length;i++) {
                            var doc = docs[i];
                            var subpath = doc.path.substr(path.length);
                            var parts = subpath.split("/");
                            if (parts.length == 1) {
                                var meta = doc.meta;
                                meta.fn = parts[0];
                                files.push(meta);
                            } else if (dirs.indexOf(parts[0]) == -1) {
                                dirs.push(parts[0]);
                            }
                        }
                        defer.resolve(dirs.concat(files));
                    }
                });
            }
        });
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

function saveLibraryEntry(type,path,meta,body) {
    var defer = when.defer();
    libCollection().then(function(libCollection) {
        libCollection.update({appname:appname,type:type,path:path},{appname:appname,type:type,path:path,meta:meta,data:body},{upsert:true},function(err) {
            if (err) {
                defer.reject(err);
            } else {
                defer.resolve();
            }
        });
    }).otherwise(function(err) {
        defer.reject(err);
    });
    return defer.promise;
}

var mongostorage = {
    init: function(_settings) {
        settings = _settings;
        appname = settings.mongoAppname || require('os').hostname();
        return db();
    },
    getFlows: function() {
        return timeoutWrap(getFlows);
    },
    saveFlows: function(flows) {
        return timeoutWrap(function(){return saveFlows(flows);});
    },

    getCredentials: function() {
        return timeoutWrap(getCredentials);
    },

    saveCredentials: function(credentials) {
        return timeoutWrap(function(){return saveCredentials(credentials);});
    },

    getSettings: function() {
        return timeoutWrap(function() { return getSettings();});
    },

    saveSettings: function(data) {
        return timeoutWrap(function() { return saveSettings(data);});
    },

    getAllFlows: function() {
        return timeoutWrap(getAllFlows);
    },

    getFlow: function(fn) {
        return timeoutWrap(function() { return getFlow(fn);});
    },

    saveFlow: function(fn,data) {
        return timeoutWrap(function() { return saveFlow(fn,data);});
    },

    getLibraryEntry: function(type,path) {
        return timeoutWrap(function() { return getLibraryEntry(type,path);});
    },
    saveLibraryEntry: function(type,path,meta,body) {
        return timeoutWrap(function() { return saveLibraryEntry(type,path,meta,body);});
    }
};

module.exports = mongostorage;
