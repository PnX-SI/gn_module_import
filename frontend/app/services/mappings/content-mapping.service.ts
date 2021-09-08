import { Injectable, Observable} from "@angular/core";
import { DataService } from "../data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../module.config";

@Injectable()
export class ContentMappingService {
  public userContentMappings;
  public newMapping: boolean = false;
  public id_mapping;
  public displayMapped: boolean;

  constructor(
    private _ds: DataService,
    private _commonService: CommonService
  ) {
    this.displayMapped = ModuleConfig.DISPLAY_MAPPED_VALUES;
  }

  getMappingNamesListMap(newContentId?, formControl?): Observable<number | null>{
    // get list of existing content mapping in the select
    return this._ds.getMappings("content").map(
      result => {
        this.userContentMappings = result
        if (newContentId) {
          const newMapping = result.find(el => {
            return el.id_mapping == newContentId
          })
          formControl.setValue(newMapping)
        }

      },
      error => {
        console.log(error);
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
        } else {
          console.log(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  createMapping(mappingForm) {
    mappingForm.reset();
    this.newMapping = true;
    this.displayMapped = true;
  }

  cancelMapping(mappingForm) {
    this.newMapping = false;
    mappingForm.controls["mappingName"].setValue("");
  }

}
