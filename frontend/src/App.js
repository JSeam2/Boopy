import React, {useState, useEffect} from 'react';
import { Button, Table, Row, Col, Empty } from 'antd';
import axios from 'axios';
import 'antd/dist/antd.css';
import './App.css';

const kvColumns = [
  {
    title: "Key",
    dataIndex: "key",
    key: "key",
  },
  {
    title: "Value",
    dataIndex: "value",
    key: "value",
  }
]

const initKvValues = [
  {
    key: "",
    value: "",
  },
]

function App() {
  const [kv, setKv] = useState(initKvValues);

  return (
    <div className="App">
      <Row>
        <Col span={12}>
          <Row>
            <Table
              columns={kvColumns}
              dataSource={kv}
            />
          </Row>
        </Col>
        <Col span={8}>
          <Row>
            <Button
              onClick={(e) => {
                e.preventDefault();
                var res = prompt("Input Key and Value Comma Separated");
                if (!res) {
                  return;
                }
                var data = res.split(',')

                // Mock data
                axios.post('/set', {
                  key: data[0],
                  value: data[1]
                })
                .then((res) => {
                  console.log(res.data);
                  alert('Message: ' + res.data.message);

                  let tempKv = Array.from(kv);
                  let flag = false;
                  for (let i = 0; i < tempKv.length; i++) {
                    if (tempKv[i].key === res.data.key) {
                      tempKv[i] = {key: res.data.key, value: res.data.value};
                      console.log(tempKv);
                      setKv(tempKv);
                      flag = true;
                      break;
                    }
                  }
                  if(!flag) {
                    setKv([...tempKv, {key: res.data.key, value: res.data.value}]);
                  }
                }, (err) => {
                  console.log(err);
                })
                // setKv([...kv, {key: data[0], value:data[1]}]);
              }}
            >
              Set
            </Button>
          </Row>
          <Row>
            <Button
              onClick={(e) => {
                e.preventDefault()
                var res = prompt("Input Key");
                if (!res) {
                  return;
                }
                var data = res.trim()
                axios.post('/get', {
                  key: data
                })
                .then((res) => {
                  console.log(res.data);
                  alert('Key: ' +  res.data.key + " Value: " + res.data.value);
                  let tempKv = Array.from(kv);
                  let flag = false;
                  for (let i = 0; i < tempKv.length; i++) {
                    if (tempKv[i].key === res.data.key) {
                      tempKv[i] = {key: res.data.key, value: res.data.value};
                      console.log(tempKv);
                      setKv(tempKv);
                      flag = true;
                      break;
                    }
                  }
                  if(!flag) {
                    setKv([...tempKv, {key: res.data.key, value: res.data.value}]);
                  }
                }, (err) => {
                  console.log(err);
                })
              }}
            >
              Get
            </Button>
          </Row>
          <Row>
            <Button
              onClick={(e) => {
                e.preventDefault()
                var res = prompt("Input Key");
                if (!res) {
                  return;
                }
                var data = res.trim()
                axios.post('/delete', {
                  key: data
                })
                .then((res) => {
                  console.log(res.data);
                  alert('Message: ' + res.data.message);
                  let tempKv = Array.from(kv);
                  for (let i = 0; i < tempKv.length; i++) {
                    if (tempKv[i].key === data) {
                      tempKv.splice(i,1);
                      console.log(tempKv);
                      setKv(tempKv);
                      break;
                    }
                  }
                }, (err) => {
                  console.log(err);
                })
              }}
            >
              Delete
            </Button>
          </Row>
        </Col>
      </Row>
    </div>
  );
}

export default App;
