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

function refreshStatus() {
    $.get('/admin/status', function (data) {
        // console.log(data);
        $("#status").text(data);
        setTimeout(refreshStatus, 10000);
    });
}

function getAndRefreshConsoleJson(path) {
    $.get(path, function (data) {
        $("#console").text(JSON.stringify(data));
    });
}

function getAndRefreshConsoleText(path) {
    $.get(path, function (data) {
        $("#console").text(data);
    });
}

function doStartProductAction(action) {
    var product = $('#product').val();
    var epoch = $('#epoch').val();
    var force = '';
    if ($('#force').is(":checked")) force = "?forceChange=true";
    getAndRefreshConsoleText(action + product + '/' + epoch + '/full' + force);
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

function goAuto() {
    getAndRefreshConsoleText('/goAuto/to/db');
}

function cancelTask() {
    getAndRefreshConsoleText('/admin/cancelTask');
}

function fullStatus() {
    getAndRefreshConsoleText('/admin/fullStatus');
}

$(document).ready(
    function () {
        getAndRefreshConsoleJson('/ping');
        refreshStatus();
        $('#go').click(doGo);
        $('#fetch').click(doFetch);
        $('#ingest').click(doIngest);
        $('#goAuto').click(goAuto);
        $('#cancelTask').click(cancelTask);
        $('#fullStatusButton').click(fullStatus);
    }
);
