import { Component, OnInit, ViewChild } from '@angular/core';
import { Router,ActivatedRoute } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../module.config';
import { MatButtonModule } from '@angular/material/button';
import { STEPPER_GLOBAL_OPTIONS } from '@angular/cdk/stepper';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html'
})


export class ImportProcessComponent implements OnInit {

  private fileName;
  private uploadResponse: JSON;
  private isUploaded: Boolean = false;
  private isUploading: Boolean = false;
  private IMPORT_CONFIG = ModuleConfig;
  private uploadForm: FormGroup;
  private importId;
  private cancelResponse;
  private synColumnNames;
  private mappingResponse: JSON;
  private syntheseForm: FormGroup;
  private isFileSelected : Boolean = false;
  private isUserError: boolean;
  private userErrors;
  private columns;

  @ViewChild('stepper') stepper: MatStepperModule;

  constructor(
    private _router: Router, 
    private _activatedRoute: ActivatedRoute,
    public _ds: DataService,
    private toastr: ToastrService,
    private _fb: FormBuilder,
  ) {

    this._activatedRoute.params.subscribe(
      //params => console.log(params)
      );

    this.uploadForm = this._fb.group({
      file: [null, Validators.required],
      encodage: [null, Validators.required],
      srid: [null, Validators.required],
      separator: [null, Validators.required]
    });

    this.syntheseForm = this._fb.group({});
  }


  ngOnInit() {
    this.importId = 'undefined';
    this.isUserError = false;
  }


  resetStepper(stepper: MatStepperModule){
    stepper.selectedIndex = 0;
  }


  onFileSelected(event) {
    console.log(event);
    this.uploadForm.patchValue({
      file: <File>event.target.files[0]
    });
    this.isFileSelected = true;
    this.fileName = this.uploadForm.get('file').value.name;
    console.log('nom du fichier = ' + this.fileName);
  }


  cancelImport() {
    console.log(this.importId);
    this._ds.cancelImport(this.importId).subscribe(
      res => {
        this.cancelResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          if (error.status = 400){
            this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
          }
          // show error message if other server error
          this.toastr.error(error.error);
        }
    },
      () => {
        console.log(this.cancelResponse);
        this.onImportList();
      }
    );       
  }


  onUpload(value) {
    this.isUploading = true;
    this.isUserError = false;
    this._ds.postUserFile(value,this._activatedRoute.params._value['datasetId'],this.importId).subscribe(
      res => {
        this.uploadResponse = res as JSON;
      },
      error => {
        this.isUploading = false;

        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          if (error.status == 500) {
            // show error message if other server error
            console.log('erreur 500 attention')
            this.toastr.error(error.error);
          }
          if (error.status == 400) {
            this.isUserError = true;
            this.userErrors = error.error;
            console.log(this.userErrors);
          }
          if (error.status == 403) {
            this.toastr.error(error.error);
          }
      },
      () => {
        this.getSynColumnNames();
        this.isUserError = false;
        this.isUploading = false;
        console.log(this.uploadResponse);
        this.importId = this.uploadResponse.importId;
        this.columns = this.uploadResponse.columns;
        console.log(this.columns);
        console.log(this.importId);
        this.isUploaded = true;
        this.stepper.next();
        this.isUploading = false;
      }
    );
  } 


  onMapping(value) {
    this._ds.postMapping(value, this.importId).subscribe(
      res => {
        this.mappingResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
            // show error message if other server error
            this.toastr.error(error.error.message);
        }
    },
      () => {
        console.log(this.mappingResponse);
      }
    );
  }


  onImportList() {
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
  }


  getSynColumnNames() {
    // attention
    this._ds.getSynColumnNames().subscribe(
      res => {
        this.synColumnNames = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          // show error message if other server error
          this.toastr.error(error.error.message);
        }
    },
      () => {
        for (let col of this.synColumnNames){
          if (col.is_nullable === 'NO') {
            this.syntheseForm.addControl(col.column_name, new FormControl('', Validators.required));
          } else {
            this.syntheseForm.addControl(col.column_name, new FormControl(''));
          }
        //console.log(this.syntheseForm);
        
      }
    );
}

  complete() {
    /*
    console.log(this.stepper);
    console.log(this.stepper.selected.completed);
    //this.stepper.selected.editable = false;
    this.stepper.completed = true;
    if (this.stepper.completed = true) {
      this.stepper.next();
    }
    */
  }

}

