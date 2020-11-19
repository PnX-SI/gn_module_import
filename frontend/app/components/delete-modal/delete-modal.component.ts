import { Component, OnInit, Input, Output, EventEmitter } from "@angular/core";
import { CommonService } from "@geonature_common/service/common.service";
import { DataService } from "../../services/data.service";
import { Router } from "@angular/router";

@Component({
  selector: "import-delete",
  templateUrl: "./delete-modal.component.html"
})
export class ModalDeleteImport implements OnInit {
  @Input() row: any;
  @Input() c: any;
  @Output() onDelete = new EventEmitter();
  constructor(
    private _commonService: CommonService,
    private _ds: DataService,
    private _router: Router
  ) { }

  ngOnInit() { }

  deleteImport() {
    console.log("deleteImport");
    this._ds.cancelImport(this.row.id_import).subscribe(
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
        } else {
          // show error message if other server error
          this._commonService.regularToaster("error", error.message);
        }
        this.onDelete.emit();
        this.c();
      }
    );
  }
}
