import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../module.config';
import { MatButtonModule } from '@angular/material/button';
import { STEPPER_GLOBAL_OPTIONS } from '@angular/cdk/stepper';

import { importIdStorage } from './importId';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html',
  providers: [{
    provide: STEPPER_GLOBAL_OPTIONS, useValue: {displayDefaultIndicatorType: false}
  }]
})


export class ImportProcessComponent implements OnInit {

  selectedFile: File = null;
  public uploadResponse: JSON;
  public IMPORT_CONFIG = ModuleConfig;
  public isFileSelected : Boolean = false; // used for disable valid button
  //step1Completed = false;
  //isLinear = true;
  public deleteResponse;

  @ViewChild('stepper') stepper: MatStepperModule;

  constructor(
    private _router: Router, 
    public _ds: DataService,
    private toastr: ToastrService,
    private _idImport: importIdStorage
  ) {}


  ngOnInit() {
  }


  resetStepper(stepper: MatStepperModule){
    stepper.selectedIndex = 0;
  }


  onFileSelected(event) {
    this.selectedFile = <File>event.target.files[0];
    this.isFileSelected = true;
    console.log('le nom du fichier uploadé par l\'utilisateur est = ' + this.selectedFile.name);
  }


  cancelImport() {
    this._ds.deleteImport(this._idImport.importId).subscribe(
      res => {
        this.deleteResponse = res as JSON;
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
        console.log(this.deleteResponse);
        this.onImportList();
      }
    );       

  }


  onUpload() {
    this._ds.postUserFile(this.selectedFile).subscribe(
      res => {
        this.uploadResponse = res as JSON;
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
        //this.isLinear = false;
        console.log(this.uploadResponse);

        //this.complete();
        // vérifier validité du csv format : en l'ouvrant (csv.read)?
        // promesse pour bloquer front en attendant que ce soit fini
      }
    );
  } 

  onImportList() {
    // effacer le fichier dans uploads (attention penser à gérer le fait que 2 utilisateurs puissent avoir le même nom de fichier?)
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
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

