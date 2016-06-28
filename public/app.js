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
        setTimeout(refreshStatusContinually, 10000);
    });
}

function get(path, refresh) {
    $.get(path, function (data) {
        $("#console").text(data);
        if (refresh)
            setTimeout(fullStatus, 250);
    });
}

function getAndRefreshConsoleJson(path) {
    $.get(path, function (data) {
        $("#console").text(JSON.stringify(data, null, 2));
    });
}

function fullStatus() {
    get('/admin/fullStatus');
}

function post(path, refresh) {
    $.post(path, function (data) {
        $("#console").text(data);
        if (refresh)
            setTimeout(fullStatus, 250);
    });
}

//-------------------------------------------------------------------------------------------------

function doStartProductAction(action) {
    var product = $('#product').val();
    var epoch = $('#epoch').val();
    var variant = $('#variant').val();
    var force = '';
    if ($('#force').is(":checked")) force = "?forceChange=true";
    if (product == '' || epoch == '' || variant == '')
        alert("Enter the product, epoch and variant");
    else
        get(action + product + '/' + epoch + '/' + variant + force, true);
}

function doGo() {
    doStartProductAction('/go/to/db/');
}

function doFetch() {
    doStartProductAction('/fetch/');
}

function doIngest() {
    doStartProductAction('/ingest/to/db/');
}

//-------------------------------------------------------------------------------------------------

function goAuto() {
    get('/goAuto/to/db', true);
}

function cancelTask() {
    get('/admin/cancelTask', true);
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

function switchCol() {
    getAndRefreshConsoleJson('/collections/list');
    var colname = $('#colname').val();
    if (colname == '')
        alert("Enter the collection name");
    else {
        colname = colname.replace(/_/g, '/');
        get('/switch/to/' + colname, true);
    }
}

$(document).ready(
    function () {
        getAndRefreshConsoleJson('/ping');
        refreshStatusContinually();
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
    }
);
