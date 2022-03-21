import {
  Component,
  AfterViewInit,
  Input,
  Output,
  EventEmitter,
} from "@angular/core";
import { CommonService } from "@geonature_common/service/common.service";

import { DataService } from "../../services/data.service";
import { PermissionsService } from "../../services/permissions.service";
import FixModel from "./need-fix.models";

@Component({
  selector: "need-fix",
  templateUrl: "need-fix.component.html",
  styleUrls: ["./need-fix.component.scss"],
})
export class NeedFixComponent implements AfterViewInit {
  public hasRights: boolean = false;

  @Input() fix: FixModel;
  @Input()
  get importId(): number {
    return this._importId;
  }
  set importId(value: number) {
    this._importId = value;
    this.updateImportId(value);
  }
  private _importId: number;

  constructor(
    private _permissionsService: PermissionsService,
    private _ds: DataService,
    private _commonService: CommonService
  ) {}

  ngAfterViewInit() {}

  updateImportId(value: number) {
    if (value) {
      this._permissionsService
        .canUserUpdate(value)
        .toPromise()
        .then((res) => (this.hasRights = res));
    }
  }

  setFix() {
    this._ds
      .setNeedFix(this.importId, this.fix.need, this.fix.comment)
      .toPromise()
      .then(() => {
        this._commonService.regularToaster("info", `Import mis à jour`);
      })
      .catch((err) => {
        console.log(err);
        this._commonService.regularToaster(
          "error",
          `Une erreur s'est produite : ${err.error}`
        );
      });
  }
}
