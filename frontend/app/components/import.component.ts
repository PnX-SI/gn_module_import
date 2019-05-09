import { Component, OnInit, ViewChild } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { AppConfig } from '@geonature_config/app.config';
import { ToastrService } from 'ngx-toastr';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { DatatableComponent } from '@swimlane/ngx-datatable';

import { ImportModalDatasetComponent } from './import-modal-dataset.component';
import { DataService } from '../services/data.service';
import { ModuleConfig } from '../module.config';

@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  public importId: JSON;
  public importHistory;
  public history;
  public empty : boolean;

  public IMPORT_CONFIG = ModuleConfig;

  @ViewChild(DatatableComponent) table: DatatableComponent;

  constructor(
    private _commonService: CommonService,
    public _ds: DataService,
    private toastr: ToastrService,
    public ngbModal: NgbModal
  ) {}


  ngOnInit() {
    this.onImportList();
  }

  onProcess() {
    this.openDatasetModal();
  }

  onImportList() {
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

  openDatasetModal() {
    const modalRef = this.ngbModal.open(ImportModalDatasetComponent, {
      centered: true, 
      size: "lg", 
      backdrop: 'static', 
      windowClass: 'dark-modal'
    });
  }
  
}

