#


### **[自然语言转化 SQL（NLSQL）](/swagger/index.html)**


#### 数据传输接口

???+ put "&thinsp; ++"PUT"++ &ensp; [/api/nl-sql/data-preprocess](/swagger/index.html)"

    === "请求 (request)"

        &nbsp; :fontawesome-solid-hashtag: &zwnj; __参数 (parameter)__
        ``` yaml linenums="1"
        taskid (必填): 17929488
        ```

        &nbsp; :fontawesome-regular-file: &zwnj; __内容 (body)__
        ``` json linenums="1"
        --8<-- "./examples/nl-sql/input-metadata.json"
        ```

        {{ read_excel('./examples/nl-sql/explanation.xlsx', sheet_name='input-metadata', engine='openpyxl') }}

        ``` json linenums="1"
        --8<-- "./examples/nl-sql/input-question.json"
        ```

        {{ read_excel('./examples/nl-sql/explanation.xlsx', sheet_name='input-question', engine='openpyxl') }}


    ===! "响应 (response)"

        &nbsp; :fontawesome-solid-check: &zwnj; __成功 (success)__
        ``` yaml linenums="1"
        code: 200
        information: "Success information"
        ```

    === "异常 (exception)"

        &nbsp; :fontawesome-solid-xmark: &zwnj; __失败 (failure)__
        ``` yaml linenums="1"
        code: 500
        information: "Error information"
        ```


#### 算法计算接口

???+ post "&thinsp; ++"POST"++ &ensp; [/api/nl-sql/algorithm-startup](/swagger/index.html)"

    === "请求 (request)"

        &nbsp; :fontawesome-solid-hashtag: &zwnj; __参数 (parameter)__
        ``` yaml linenums="1"
        taskid (必填): 17929488
        version (必填): [ '3.5', '3.5-16k', '4', '4-32k' ]
        ```

    ===! "响应 (response)"

        &nbsp; :fontawesome-solid-check: &zwnj; __成功 (success)__
        ``` yaml linenums="1"
        code: 200
        information: "Success information"
        ```

    === "异常 (exception)"

        &nbsp; :fontawesome-solid-xmark: &zwnj; __失败 (failure)__
        ``` yaml linenums="1"
        code: 500
        information: "Error information"
        ```


#### 查询接口

???+ get "&thinsp; ++"GET"++ &ensp; [/api/nl-sql/data-download](/swagger/index.html)"

    === "请求 (request)"

        &nbsp; :fontawesome-solid-hashtag: &zwnj; __参数 (parameter)__
        ``` yaml linenums="1"
        taskid (必填): 17929488
        schema (必填): [ system/logs, input/results, output/results ]
        ```

    ===! "响应 (system-logs)"
 
        &nbsp; :fontawesome-solid-check: &zwnj; __成功 (success)__
        ``` yaml linenums="1"
        [2023-01-01 00:00:01] TaskID @ FZ7veHWozQdPWig3UGZUL28o <100>: Log information
        [2023-01-01 00:00:02] TaskID @ FZ7veHWozQdPWig3UGZUL28o <100>: Log information
        ```

    === "响应 (input-texts)"
 
        &nbsp; :fontawesome-solid-check: &zwnj; __成功 (success)__
        ``` json linenums="1"
        --8<-- "./examples/nl-sql/input-metadata.json"
        ```

        {{ read_excel('./examples/nl-sql/explanation.xlsx', sheet_name='input-metadata', engine='openpyxl') }}


        ``` json linenums="1"
        --8<-- "./examples/nl-sql/input-question.json"
        ```

        {{ read_excel('./examples/nl-sql/explanation.xlsx', sheet_name='input-question', engine='openpyxl') }}

    === "响应 (output-results)"
 
        &nbsp; :fontawesome-solid-check: &zwnj; __成功 (success)__
        ``` json linenums="1"
        --8<-- "./examples/nl-sql/output-results.json"
        ```

        {{ read_excel('./examples/nl-sql/explanation.xlsx', sheet_name='output-results', engine='openpyxl') }}

    === "异常 (exception)"

        &nbsp; :fontawesome-solid-xmark: &zwnj; __失败 (failure)__
        ``` yaml linenums="1"
        code: 500
        information: "Error information"
        ```
