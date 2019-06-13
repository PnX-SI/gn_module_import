import { Component, OnInit, ViewChild } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { AppConfig } from '@geonature_config/app.config';
import { ToastrService } from 'ngx-toastr';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { DatatableComponent } from '@swimlane/ngx-datatable';
import { Router } from "@angular/router";

import { ImportModalDatasetComponent } from './import-modal-dataset.component';
import { DataService } from '../services/data.service';
import { ModuleConfig } from '../module.config';

@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  private importHistory;
  private deletedStep1;
  private history;
  private empty : boolean;
  private selected = [];
  private IMPORT_CONFIG = ModuleConfig;
  private rowData;

  @ViewChild(DatatableComponent) table: DatatableComponent;

  constructor(
    private _commonService: CommonService,
    private _ds: DataService,
    private toastr: ToastrService,
    private ngbModal: NgbModal,
    private _router: Router
  ) {}


  ngOnInit() {
    this.onImportList();
    this.onDelete_aborted_step1();
    // faire promesse pour structurer le déroulement de ces 2 appels
  }


  private onProcess() {
    this.openDatasetModal();
  }


  private onImportList() {
    this._ds.getImportList().subscribe(
      res => {
        this.importHistory = res;
        this.history = this.importHistory.history;
        this.empty = this.importHistory.empty;
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
        console.log(this.importHistory);
      }
    );
  }


  private openDatasetModal() {
    const modalRef = this.ngbModal.open(ImportModalDatasetComponent, {
      centered: true, 
      size: "lg", 
      backdrop: 'static', 
      windowClass: 'dark-modal'
    });
  }


  private go_to_step() {
    if (this.rowData != "undefined") {
      console.log(this.rowData);
      // create TImports interface and push this.rowData in rowData interface
      //this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}/process/${this.rowData.id_dataset}`]);
    }
  }
  
  
  private onActivate(event) {
      this.rowData = event.row;
      return this.rowData;
  }


  private onDelete_aborted_step1() {
    this._ds.delete_aborted_step1().subscribe(
      res => {
        this.deletedStep1 = res;
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
        console.log(this.deletedStep1);
      }
    ); 
  }
  

}

