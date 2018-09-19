import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import axios from "axios";
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend} from "recharts";
import ResponsiveContainer from "recharts/es6/component/ResponsiveContainer";

class CPULineChart extends Component {
    render () {
        var current = 0;
        if(this.props.data.slice(-1)[0] !== undefined){
            current = this.props.data.slice(-1)[0].cpu
        }
        return (
            <div className="uk-card uk-card-default">
                <div className="uk-card-header" style={{"padding": "10px", "margin": "0px", "text-align": "center"}}>
                    <h3 className="uk-card-title uk-margin-remove-bottom" style={{"font-size":"20px"}} >CPU Load: {current}</h3>
                </div>
                <div className="uk-card-body" style={{"padding":"0px"}}>
                    <div style={{"width":"100%", "height":"150px"}}>
                        <ResponsiveContainer>
                            <LineChart data={this.props.data}>
                                <CartesianGrid stroke="#eee" strokeDasharray="2 2"/>
                                <Line type="monotone" dataKey="cpu" stroke="#48435c" strokeWidth={2} dot={false}/>
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        );
    }
}
class MemoryLineChart extends Component {
    render () {
        var current = 0;
        if(this.props.data.slice(-1)[0] !== undefined){
            current = this.props.data.slice(-1)[0].memory
        }
        return (
            <div className="uk-card uk-card-default">
                <div className="uk-card-header" style={{"padding": "10px", "margin": "0px", "text-align": "center"}}>
                    <h3 className="uk-card-title uk-margin-remove-bottom" style={{"font-size":"20px"}} >Memory Usage: {current} MB</h3>
                </div>
                <div className="uk-card-body" style={{"padding":"0px"}}>
                    <div style={{"width":"100%", "height":"150px"}}>
                        <ResponsiveContainer>
                            <LineChart data={this.props.data}>
                                <CartesianGrid stroke="#eee" strokeDasharray="2 2"/>
                                <Line type="monotone" dataKey="memory" stroke="#7daa92" strokeWidth={2} dot={false}/>
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        );
    }
}
class JobsLineChart extends Component {
    render () {
        var current = 0;
        if(this.props.data.slice(-1)[0] != undefined){
            current = this.props.data.slice(-1)[0].jobsproccessed
        }
        return (
            <div className="uk-card uk-card-default">
                <div className="uk-card-header" style={{"padding": "10px", "margin": "0px", "text-align": "center"}}>
                    <h3 className="uk-card-title uk-margin-remove-bottom" style={{"font-size":"20px"}} >Job Processed: {current}</h3>
                </div>
                <div className="uk-card-body" style={{"padding":"0px"}}>
                    <div style={{"width":"100%", "height":"150px"}}>
                        <ResponsiveContainer>
                            <LineChart data={this.props.data}>
                                <CartesianGrid stroke="#eee" strokeDasharray="2 2"/>
                                <Line type="monotone" dataKey="jobsproccessed" stroke="#8e4a49" strokeWidth={2} dot={false}/>
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        );
    }
}

class JobForm extends Component{

    state = {
        name : "",
        type:"",
        model:[],
        parmas:{}
    }

    getParams(type){
        let p = [];
        this.props.params.forEach(function(element) {
            console.log(element.type, type);
            if(element.type === type){
                console.log(element.params, type);
                p = element.params;
            }
        });
        return p;
    }

    constructor(){
        super()
        //this.state.type = this.props.options[0]
    }
    componentDidMount() {
        this.setState({
            type:this.props.options[0],
            model:this.getParams(this.props.options[0])
        })
        console.log("hello",this.props.options, this.props.params, this.state)
    }

    inputChangeHandler = (event) => {
        if(event.target.name === "type"){
            let state = this.state;
            state.model.forEach(function(element) {
                state.parmas[element] = ""
            });
            this.setState({
                parmas: state.parmas,
                [event.target.name]: event.target.value,
            });

            this.setState({
                model:this.getParams(event.target.value),
            });

        }else{
            this.setState({
                [event.target.name]: event.target.value
            });
        }
    }

    inputChangeHandlerParams = (event) => {
        let parmas = this.state.parmas;
        parmas[event.target.name] = event.target.value;
        this.setState({parmas:parmas});
    }


    submitChangeHandler = (event) => {
        let model = Object.assign({}, this.state);
        delete model.model
        model.parmas = JSON.stringify(model.parmas);
        event.preventDefault();

        axios.post('data',model)
            .then(function (response) {
                console.log(response);
            })
            .catch(function (error) {
                console.log(error);
            });
        this.props.cancel()
    }

    cancelChangeHandler = () => {
        let state = this.state;
        state.model.forEach(function(element) {
            state.parmas[element] = ""
        });
        this.setState({
            parmas: state.parmas,
            name:"",
        });
        this.setState({
            model:this.getParams(this.props.options[0])
        });

        this.props.cancel()
    }

    render(){
        return (
            <div>
                <form onSubmit={this.submitChangeHandler}>
                    <fieldset className="uk-fieldset">
                        <h2 className="uk-h3 tm-heading-fragment">Mange Jobs</h2>
                        <div className="uk-margin">
                            <input className="uk-input" type="text" placeholder="Job Name" name="name" value={this.state.name} onChange={this.inputChangeHandler}/>
                        </div>
                        <div className="uk-margin">
                            <select className="uk-select" name="type" value={this.state.type} onChange={this.inputChangeHandler}>
                                {this.props.options.map(function(d, idx){
                                    return (<option>{d}</option>)
                                })}
                            </select>
                        </div>

                        {this.state.model.map((d) => {
                            return (
                                <div className="uk-margin">
                                    <input className="uk-input" type="text" placeholder={d} name={d} value={this.state.parmas[d]} onChange={this.inputChangeHandlerParams}/>
                                </div>
                            )
                        })}
                    </fieldset>
                    <button className="uk-button uk-button-default"  onClick={this.cancelChangeHandler} style={{"float":"right", "margin-left":"5px"}}>Cancel</button>
                    <input type="submit" className="uk-button uk-button-secondary" style={{"float":"right"}} value="submit"/>
                </form>
                <hr className="uk-divider-icon" style={{"margin-top":"42px"}}/>
            </div>
        )
    }
}

class App extends Component {
    state = {
        options: [],
        params:{},
        jobs:[],
        worker:"",
        metrics: [],
        toggledisplay: false
    }

    toggle = ()=>{
       this.setState({toggledisplay:!this.state.toggledisplay})
    }

    loadData = ()=>{
        let that = this;
        axios.get("data")
            .then(function(response) {
                let metric = response.data.Metric;
                let date =  Date.now();

                if(that.state.metrics.length > 400){
                    that.state.metrics.shift()
                }
                metric.time = new Date().toTimeString().replace(/.*(\d{2}:\d{2}:\d{2}).*/, "$1");

                let options = [];
                response.data.JobParmas.forEach(function(element) {
                    options.push(element.type)
                });



                that.setState({
                    "jobs":response.data.Jobs,
                    "metrics":  [...that.state.metrics, metric],
                    "worker": metric.Hostname,
                    "params":  response.data.JobParmas,
                    "options": options,
                    "services":response.data.Service,
                })
                console.log(that.state);

            })
            .catch(function(error) {
                console.log(error);
            });
    }


    componentDidMount() {
        let that = this;
        this.interval = setInterval(() => {
            that.loadData();
        }, 2000);
    }
    componentWillUnmount() {
        clearInterval(this.interval);
    }

    constructor() {
        super();
        this.loadData();

    }

    remove = (d) => {
        const arrayCopy = this.state.jobs.filter((row) => row.name !== d.name);
        let that = this;
        axios.delete("data", { data: d })
            .then(function(response) {
                that.setState({jobs: arrayCopy});
            })
            .catch(function(error) {});
    };




  render() {
    return (
        <div className="App">

            <nav className="uk-navbar-container uk-margin" uk-navbar>
                <div className="uk-navbar-center">
                    <a className="uk-navbar-item uk-logo" href="#" style={{"margin":"auto"}}>OpenDataLab Ingestion Engine</a>
                </div>
            </nav>
            <div className="uk-container">
                <div className="uk-overflow-auto">
                    <table className="uk-table uk-table-middle uk-table-divider">
                        <tr>
                            <td style={{"width":"33%"}}>
                                <CPULineChart data={this.state.metrics}/>
                            </td>
                            <td style={{"width":"33%"}}>
                                <MemoryLineChart data={this.state.metrics}/>
                            </td>
                            <td style={{"width":"33%"}}>
                                <JobsLineChart data={this.state.metrics}/>
                            </td>
                        </tr>
                    </table>

                    <hr className="uk-divider-icon"/>
                    {this.state.toggledisplay && <JobForm params={this.state.params} options={this.state.options} cancel={this.toggle.bind(this)}/>}

                    <div>
                        <button className="uk-button uk-button-primary" onClick={this.toggle.bind(this)} style={{"float":"right"}}>Create Job</button>
                        <h2 className="uk-h3 tm-heading-fragment">Jobs List</h2>

                      <table className="uk-table uk-table-hover uk-table-middle uk-table-divider">
                          <thead>
                          <tr>
                              <th>Job Name</th>
                              <th>Job Type</th>
                              <th>Job Worker</th>
                              <th>Last Updated</th>
                              <th className="uk-width-small"></th>
                          </tr>
                          </thead>
                          <tbody>
                          {this.state.jobs.map(function(d, idx){
                              return (
                                  <tr>
                                      <td>{d.name}</td>
                                      <td>{d.type}</td>
                                      <td>{d.id}</td>
                                      <td>{d.time}</td>
                                      <td>
                                          <button onClick={() => this.remove(d)} className="uk-button uk-button-danger" type="button"><span
                                              uk-icon="icon:  trash"></span></button>

                                      </td>
                                  </tr>
                              )
                          },this)}
                          </tbody>
                      </table>
                    </div>

                </div>
          </div>
            <div id="footer">
                <div style={{"margin-right":"20px"}}>
                <p  style={{"margin":"0px"}}>Worker: {this.state.worker} Version: 0.0.1</p>
                </div>
            </div>
      </div>
    );
  }
}

export default App;