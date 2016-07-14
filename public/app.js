/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

function refreshStatusContinually() {
    $.get('/admin/status', function (data) {
        // console.log(data);
        $("#status").text(data);
        setTimeout(refreshStatusContinually, 1000);
    });
}

function consoleText(text) {
    $("#console").removeClass('error').text(text);
}

function consoleError(text) {
    $("#console").addClass('error').text(text);
}

function ajax(method, path, success) {
    consoleText('Pending...');
    $.ajax({
        method: method,
        url: path,
        success: success,
        error: function (xhr, status, error) {
            console.log(status);
            console.log(error);
            consoleError(status + '\n' + error);
        }
    });
}

function get(path, refresh) {
    ajax('GET', path, function (data) {
        consoleText(data);
        if (refresh)
            setTimeout(fullStatus, 250);
    });
}

function post(path, refresh) {
    ajax('POST', path, function (data) {
        consoleText(data);
        if (refresh)
            setTimeout(fullStatus, 250);
    });
}

function getAndRefreshConsoleJson(path) {
    ajax('GET', path, function (data) {
        consoleText(JSON.stringify(data, null, 2));
    });
}

function fullStatus() {
    get('/admin/fullStatus');
}

//-------------------------------------------------------------------------------------------------

function doStartProductAction(action) {
    var product = $('#product').val();
    var epoch = $('#epoch').val();
    var variant = $('#variant').val();
    var bulkSize = '?bulkSize=' + $('#bulkSize').val();
    var loopDelay = '&loopDelay=' + $('#loopDelay').val();
    var force = '';
    if ($('#force').is(":checked")) force = "&forceChange=true";
    if (product == '' || epoch == '' || variant == '')
        alert("Enter the product, epoch and variant");
    else
        get(action + product + '/' + epoch + '/' + variant + bulkSize + loopDelay + force, true);
}

function doGo() {
    doStartProductAction('/go/via/file/to/db/');
}

function doFetch() {
    doStartProductAction('/fetch/to/file/');
}

function doIngest() {
    doStartProductAction('/ingest/from/file/to/db/');
}

//-------------------------------------------------------------------------------------------------

function goAuto() {
    get('/goAuto/via/file/to/db', true);
}

function cancelTask() {
    get('/admin/cancelTask', true);
}

function remoteTree() {
    get('/fetch/showRemoteTree', false);
}

function cleanFs() {
    post('/fetch/clean', true);
}

function cleanCol() {
    post('/collections/clean', true);
}

function listCol() {
    getAndRefreshConsoleJson('/collections/list');
}

function ping() {
    getAndRefreshConsoleJson('/ping');
}

function switchCol() {
    var colname = $('#colname').val();
    if (colname == '')
        alert("Enter the collection name");
    else {
        colname = colname.replace(/_/g, '/');
        get('/switch/to/' + colname, true);
    }
}

function dropCol() {
    var colname = $('#colname').val();
    if (colname == '')
        alert("Enter the collection name");
    else {
        ajax('DELETE', '/collections/' + colname, function (data) {
            setTimeout(listCol, 250);
        });
    }
}

function dirTree() {
    var dir = $('#dirRoot').val();
    if (dir != '')
        dir = "root=" + dir + "&";
    var max = $('#dirMax').val();
    if (max != '')
        max = "max=" + max;
    ajax('GET', '/admin/dirTree?' + dir + max, function (data) {
        consoleText(data);
    });
}

function showLog() {
    var dir = $('#logDir').val();
    if (dir != '')
        dir = "dir=" + dir;
    ajax('GET', '/admin/showLog?' + dir, function (data) {
        consoleText(data);
    });
}

function toggleFieldset(id) {
    $(id + ' div').toggle();
    $(id).toggleClass('visible');
}

$(document).ready(
    function () {
        // buttons
        $('#fullStatusButton').click(fullStatus);
        $('#go').click(doGo);
        $('#fetch').click(doFetch);
        $('#ingest').click(doIngest);
        $('#goAuto').click(goAuto);
        $('#cancelTask').click(cancelTask);
        $('#cleanFs').click(cleanFs);
        $('#cleanCol').click(cleanCol);
        $('#listCol').click(listCol);
        $('#switchCol').click(switchCol);
        $('#dropCol').click(dropCol);
        $('#remoteTree').click(remoteTree);
        $('#ping').click(ping);
        $('#dirTree').click(dirTree);
        $('#showLog').click(showLog);
        // view toggles
        $('#fileActions legend').click(function() { toggleFieldset('#fileActions')} );
        $('#collectionActions legend').click(function() { toggleFieldset('#collectionActions')});
        $('#infoActions legend').click(function() { toggleFieldset('#infoActions')});
        getAndRefreshConsoleJson('/ping');
        refreshStatusContinually();
    }
);
