import { Component, OnInit } from '@angular/core';
import { NgbModal, NgbActiveModal, ModalDismissReasons } from "@ng-bootstrap/ng-bootstrap";
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { ToastrService } from 'ngx-toastr';
import { Router } from "@angular/router";

import { DataService } from '../services/data.service';
import { ModuleConfig } from '../module.config';
import { ImportComponent } from './import.component';
//import { importIdStorage } from './importId';

@Component({
    selector: 'pnx-import-modal-dataset',
    templateUrl: 'import-modal-dataset.component.html',
    styleUrls: ["./import-modal-dataset.component.scss"]
  })

  export class ImportModalDatasetComponent implements OnInit {

    public selectDatasetForm: FormGroup;
    public userDatasetsResponse: JSON; // server response for getUserDatasets
    public datasetResponse: JSON; // server response for postDatasetResponse (= post the dataset name from the 'selectDatasetForm' form)
    public isUserDatasetError: Boolean = false; // true if user does not have any declared dataset
    public datasetError;
    public IMPORT_CONFIG = ModuleConfig;


    constructor(
        public activeModal: NgbActiveModal,
        private _fb: FormBuilder,
        public _ds: DataService,
        private toastr: ToastrService,
        private _router: Router,
        //private _idImport: importIdStorage
      ) {
        this.selectDatasetForm = this._fb.group({
          dataset: ['', Validators.required]
        });
      }


    ngOnInit() {
      this.getUserDataset();
    }


    closeModal() {
      this.activeModal.close();  
    }


    onSubmit(value) {
      console.log(value.dataset);
      //this._idImport.datasetId = value.dataset;
      this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}/process/${value.dataset}`]);
      this.closeModal();
      
      /*
      this._ds.postDataset(value).subscribe(
        res => {
          this.datasetResponse = res as JSON;
      },
        error => {
          if (error.statusText === 'Unknown Error') {
            // show error message if no connexion
            this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
          } else {
            // show error message if other server error
            this.toastr.error(error.error);
          }
      },
        () => {
          console.log(this.datasetResponse);
          this._idImport.datasetId = this.datasetResponse.id_dataset;
          this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}/process`]);
          this.closeModal();
        }
      );
      */
  
    } 


    getUserDataset() {
      // get list of all declared dataset of the user
      this._ds.getUserDatasets().subscribe(
        result => {
          this.userDatasetsResponse = result;
        },
        err => {
          if (err.statusText === 'Unknown Error') {
            // show error message if no connexion
            this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
          } else {
            // show error message if user does not have any declared dataset
            if (err.status == 400) {
              this.isUserDatasetError = true;
              this.datasetError = err.error;
            } else {
              // show error message if other server error
              this.toastr.error(err.error);
            }
          }
        },
        () => {
          console.log(this.userDatasetsResponse);
        }
      );
    }
    



  }
    




