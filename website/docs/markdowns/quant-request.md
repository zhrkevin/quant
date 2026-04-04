#

???+ POST "++'POST'++ ++'请求 Request'++ &ensp; [/quant/api/update-database/startup](/quant/swagger/index.html)"

    &nbsp; :fontawesome-solid-hashtag: &zwnj; __参数 (parameter)__
    ``` yaml
    taskid: pis5gfsboWbGhuKgwSxYwhh9
    ```
    ``` yaml
    callback: http://0.0.0.0:10000/callbacks/algorithms
    ```

    | 参数      |  类型   | 必填 | 描述    |
    | --------- | ------ | --- | ------- |
    | taskid    | string | 是  | 任务 ID  |
    | callback  | string | 是  | 回调接口  |

    === ":fontawesome-solid-check: &zwnj; 正常响应 (response)"
        ``` yaml linenums="1"
        # 成功 (success)
        code: 200
        information: "Success information"
        ```

    === ":fontawesome-solid-xmark: &zwnj; 异常响应 (exception)"
        ``` yaml linenums="1"
        # 网络异常 (network error)
        code: 400
        information: "Error information"
        ```

        ``` yaml linenums="1"
        # 算法异常 (internal error)
        code: 500
        information: "Error information"
        ```

    ===! ":fontawesome-solid-check: &zwnj; 算法成功回调 (success callback)"
        ``` json linenums="1"
        {
            code: 600
            taskid: "pis5gfsboWbGhuKgwSxYwhh9"
            information: "Success information"
        }
        ```

    === ":fontawesome-solid-xmark: &zwnj; 异常错误回调 (exception callback)"
        ``` json linenums="1"
        {
            code: 500
            taskid: "pis5gfsboWbGhuKgwSxYwhh9"
            information: "Error information"
        }
        ```
