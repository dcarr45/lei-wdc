(function () {
    var connector = tableau.makeConnector();

    connector.init = function(initCallback) {
    	initCallback();
    	tableau.submit();
    }

    connector.getSchema = function (schemaCallback) {
    	tableau.log("Hello WDC!");
	    var cols = [{
	        id: "lei",
	        alias: "LEI",
	        dataType: tableau.dataTypeEnum.string
	    }, {
	        id: "name",
	        alias: "Legal Entity",
	        dataType: tableau.dataTypeEnum.string
	    }, {
	        id: "status",
	        alias: "Entity Status",
	        dataType: tableau.dataTypeEnum.string
	    }, {
	        id: "renewal",
	        alias: "Next Renewal Date",
	        dataType: tableau.dataTypeEnum.datetime
	    }];

	    var tableSchema = {
	        id: "leiFeed",
	        alias: "LEI statuses from GLEIF",
	        columns: cols
	    };

	    tableau.log("Calling schemaCallback");
	    schemaCallback([tableSchema]);
    };

    connector.getData = function (table, doneCallback) {
    	tableau.log("Fetching LEI data");
    	var leis = ["5493008DK1WDY4NF3776", "549300F6VHCLKCUWDT34"],
    		batches = [],
    		i = 0
    		batchSize = 1;

    	while (i < leis.length) {
    		batches.push(leis.slice(i, i += batchSize));
    	}

    	var promises = [];

    	batches.forEach(function(batch, index) {
	    	var promise = new Promise(function(resolve, reject) {
	    		var url = "https://leilookup.gleif.org/api/v2/leirecords?lei=" + batch.join(",");
		    	$.getJSON(url, function(resp) {
		    		tableau.log("Got response");
					var tableData = [];
					for (var i = 0; i < resp.length; i++) {
						tableData.push({
							"lei": resp[i].LEI["$"],
							"name": resp[i].Entity.LegalName["$"],
							"status": resp[i].Entity.EntityStatus["$"],
							"renewal": resp[i].Registration.NextRenewalDate["$"]
						});
					}
					table.appendRows(tableData);
					tableau.log("Done with batch");
					resolve();
				});
			});
			promises.push(promise);
	    });

	    Promise.all(promises).then(function(values) {
	    	doneCallback();
	    });
    };

    tableau.registerConnector(connector);
})();
